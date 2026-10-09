//go:build ignore

// es-forkcov: coverage accounting for the Enter Space rclone fork.
//
// Upstream rclone tests every release with its own suite, so Enter Space
// only measures what the FORK changes. This tool has two subcommands:
//
//	go run bin/es-forkcov.go gen [-base v1.75.1] [-out bin/es-forkcov-include.txt]
//
// turns `git diff <base>..HEAD` into an include list: a file the fork ADDED
// is counted whole; a file the fork MODIFIED is counted only inside the
// functions its hunks touch. Test files, files whose build constraints don't
// match this host (e.g. icloud_other.go on darwin) and bin/ itself are left
// out. Regenerate it after every upstream merge (bin/es-fork-coverage.sh
// --regen), and commit the result.
//
//	go run bin/es-forkcov.go report -profile cover.out [-include list] [-module M] [-target name] [-json]
//
// reads a `go test -coverprofile` and prints covered/executable LINES for the
// included code only (a line is executable when a profile block spans it,
// covered when any block spanning it ran), per file and in total. With -json
// it prints a Soren `soren-coverage: {...}` line.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: es-forkcov gen|report [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "gen":
		gen(os.Args[2:])
	case "report":
		report(os.Args[2:])
	default:
		fmt.Fprintln(os.Stderr, "unknown subcommand", os.Args[1])
		os.Exit(2)
	}
}

// ---------------------------------------------------------------- gen

type span struct {
	from, to int // inclusive line range; 0,0 = whole file
	name     string
}

func git(args ...string) string {
	out, err := exec.Command("git", args...).Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "git %s: %v\n", strings.Join(args, " "), err)
		os.Exit(1)
	}
	return string(out)
}

var hunkRe = regexp.MustCompile(`^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@`)

// changedLines returns the new-side line numbers a diff touches. A pure
// deletion marks the line it happened at, so the enclosing function counts.
func changedLines(base, file string) []int {
	var lines []int
	for _, l := range strings.Split(git("diff", "-U0", base, "HEAD", "--", file), "\n") {
		m := hunkRe.FindStringSubmatch(l)
		if m == nil {
			continue
		}
		start, _ := strconv.Atoi(m[1])
		n := 1
		if m[2] != "" {
			n, _ = strconv.Atoi(m[2])
		}
		if n == 0 {
			lines = append(lines, start, start+1)
			continue
		}
		for i := 0; i < n; i++ {
			lines = append(lines, start+i)
		}
	}
	return lines
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	t := fd.Recv.List[0].Type
	star := ""
	if s, ok := t.(*ast.StarExpr); ok {
		star, t = "*", s.X
	}
	if ix, ok := t.(*ast.IndexExpr); ok {
		t = ix.X
	}
	if id, ok := t.(*ast.Ident); ok {
		return "(" + star + id.Name + ")." + fd.Name.Name
	}
	return fd.Name.Name
}

func gen(args []string) {
	fl := flag.NewFlagSet("gen", flag.ExitOnError)
	base := fl.String("base", "", "upstream release tag the fork last merged (default: newest v* tag merged into HEAD)")
	out := fl.String("out", "bin/es-forkcov-include.txt", "include list to write")
	_ = fl.Parse(args)
	if *base == "" {
		*base = strings.TrimSpace(strings.SplitN(git("tag", "--merged", "HEAD", "--sort=-v:refname", "--list", "v*"), "\n", 2)[0])
	}
	baseSHA := strings.TrimSpace(git("rev-parse", *base+"^{commit}"))
	headSHA := strings.TrimSpace(git("rev-parse", "HEAD"))

	type entry struct {
		file  string
		spans []span
		note  string
	}
	var entries []entry
	for _, l := range strings.Split(strings.TrimSpace(git("diff", "--name-status", "--no-renames", *base, "HEAD", "--", "*.go")), "\n") {
		f := strings.Fields(l)
		if len(f) < 2 {
			continue
		}
		status, file := f[0], f[1]
		if status == "D" || strings.HasSuffix(file, "_test.go") || strings.HasPrefix(file, "bin/") {
			continue
		}
		ok, err := build.Default.MatchFile(filepath.Dir(file), filepath.Base(file))
		if err != nil || !ok {
			entries = append(entries, entry{file: file, note: "build constraints exclude this host"})
			continue
		}
		if status == "A" {
			entries = append(entries, entry{file: file, spans: []span{{0, 0, "*"}}})
			continue
		}
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", file, err)
			os.Exit(1)
		}
		touched := map[int]bool{}
		for _, n := range changedLines(*base, file) {
			touched[n] = true
		}
		var spans []span
		for _, d := range af.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			from, to := fset.Position(fd.Pos()).Line, fset.Position(fd.End()).Line
			for n := from; n <= to; n++ {
				if touched[n] {
					spans = append(spans, span{from, to, funcName(fd)})
					break
				}
			}
		}
		if len(spans) == 0 {
			entries = append(entries, entry{file: file, note: "declarations only (no statements changed)"})
			continue
		}
		entries = append(entries, entry{file: file, spans: spans})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].file < entries[j].file })

	var b strings.Builder
	fmt.Fprintf(&b, "# Enter Space fork coverage include list — GENERATED, do not edit.\n")
	fmt.Fprintf(&b, "# Regenerate after every upstream merge: bin/es-fork-coverage.sh --regen\n")
	fmt.Fprintf(&b, "# base %s %s\n# head %s\n", *base, baseSHA, headSHA)
	fmt.Fprintf(&b, "# <file> <from>-<to> <func> | <file> * (fork-added, whole file)\n")
	for _, e := range entries {
		if e.note != "" {
			fmt.Fprintf(&b, "# skipped %s: %s\n", e.file, e.note)
			continue
		}
		for _, s := range e.spans {
			if s.from == 0 {
				fmt.Fprintf(&b, "%s *\n", e.file)
			} else {
				fmt.Fprintf(&b, "%s %d-%d %s\n", e.file, s.from, s.to, s.name)
			}
		}
	}
	if err := os.WriteFile(*out, []byte(b.String()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s (%d files, base %s)\n", *out, len(entries), *base)
}

// ---------------------------------------------------------------- report

type fileCov struct {
	exec, cov map[int]bool
}

func readInclude(path string) map[string][]span {
	inc := map[string][]span{}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		fs := strings.Fields(l)
		if len(fs) < 2 {
			continue
		}
		if fs[1] == "*" {
			inc[fs[0]] = append(inc[fs[0]], span{0, 0, "*"})
			continue
		}
		var a, z int
		if _, err := fmt.Sscanf(fs[1], "%d-%d", &a, &z); err != nil {
			continue
		}
		name := ""
		if len(fs) > 2 {
			name = fs[2]
		}
		inc[fs[0]] = append(inc[fs[0]], span{a, z, name})
	}
	return inc
}

func inSpans(spans []span, line int) bool {
	for _, s := range spans {
		if s.from == 0 || (line >= s.from && line <= s.to) {
			return true
		}
	}
	return false
}

func report(args []string) {
	fl := flag.NewFlagSet("report", flag.ExitOnError)
	profile := fl.String("profile", "cover.out", "go test -coverprofile output")
	include := fl.String("include", "bin/es-forkcov-include.txt", "include list (see gen)")
	module := fl.String("module", "github.com/rclone/rclone", "module path the profile's file names start with")
	target := fl.String("target", "rclone-fork (fork-only)", "Soren target name")
	asJSON := fl.Bool("json", false, "print a soren-coverage line")
	perFile := fl.Bool("files", true, "print a per-file table")
	_ = fl.Parse(args)

	inc := readInclude(*include)
	files := map[string]*fileCov{}
	pf, err := os.Open(*profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pf.Close()
	// name.go:line.col,line.col numStmt count
	re := regexp.MustCompile(`^(.+):(\d+)\.\d+,(\d+)\.\d+ (\d+) (\d+)$`)
	sc := bufio.NewScanner(pf)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		m := re.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(m[1], *module), "/")
		spans, ok := inc[rel]
		if !ok {
			continue
		}
		from, _ := strconv.Atoi(m[2])
		to, _ := strconv.Atoi(m[3])
		stmts, _ := strconv.Atoi(m[4])
		count, _ := strconv.Atoi(m[5])
		if stmts == 0 {
			continue
		}
		fc := files[rel]
		if fc == nil {
			fc = &fileCov{exec: map[int]bool{}, cov: map[int]bool{}}
			files[rel] = fc
		}
		for n := from; n <= to; n++ {
			if !inSpans(spans, n) {
				continue
			}
			fc.exec[n] = true
			if count > 0 {
				fc.cov[n] = true
			}
		}
	}
	var names []string
	for f := range inc {
		names = append(names, f)
	}
	sort.Strings(names)
	totC, totE := 0, 0
	var missing []string
	for _, f := range names {
		fc := files[f]
		if fc == nil {
			missing = append(missing, f)
			continue
		}
		c, e := len(fc.cov), len(fc.exec)
		totC += c
		totE += e
		if *perFile {
			fmt.Printf("%6.1f%%  %5d/%-5d  %s\n", pct(c, e), c, e, f)
		}
	}
	for _, f := range missing {
		fmt.Printf("   n/a   (not in profile — package not tested?)  %s\n", f)
	}
	fmt.Printf("TOTAL %s: %.1f%% (%d/%d lines)\n", *target, pct(totC, totE), totC, totE)
	if *asJSON {
		j, _ := json.Marshal(map[string]any{"covered": totC, "executable": totE,
			"targets": []map[string]any{{"name": *target, "covered": totC, "executable": totE}}})
		fmt.Printf("soren-coverage: %s\n", j)
	}
	if fl := os.Getenv("ES_FORKCOV_FLOOR"); fl != "" {
		floor, _ := strconv.ParseFloat(fl, 64)
		if pct(totC, totE) < floor {
			fmt.Printf("FAIL: %s coverage %.1f%% is below the floor %.1f%%\n", *target, pct(totC, totE), floor)
			os.Exit(3)
		}
	}
}

func pct(c, e int) float64 {
	if e == 0 {
		return 0
	}
	return float64(c) * 100 / float64(e)
}
