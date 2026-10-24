package union

import (
	"context"
	"fmt"

	"github.com/rclone/rclone/backend/union/upstream"
	"github.com/rclone/rclone/fs"
)

// OpenRangeWriter opens an existing object for writes at arbitrary offsets
// on the one upstream that holds it.
//
// The union advertises the feature only when every upstream has it
// (features.Mask). An object that lives on several upstreams (a mirroring
// policy put it there) is refused with ErrorPermissionDenied, as is one on
// an upstream the action policy would not write to: a ranged write to one
// copy would leave the copies diverging, so such a file is replaced whole,
// as before. The VFS turns the refusal into EPERM on the open.
func (f *Fs) OpenRangeWriter(ctx context.Context, remote string) (fs.RangeWriter, error) {
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	o := obj.(*Object)
	candidates := o.candidates()
	if len(candidates) != 1 {
		return nil, fmt.Errorf("%q is on %d upstreams; an in-place edit needs exactly one: %w", remote, len(candidates), fs.ErrorPermissionDenied)
	}
	entries, err := f.actionEntries(candidates...)
	if err != nil {
		return nil, err // ErrorPermissionDenied when the upstream is read-only
	}
	if len(entries) != 1 {
		return nil, fmt.Errorf("the action policy names %d upstreams for %q; an in-place edit needs exactly one: %w", len(entries), remote, fs.ErrorPermissionDenied)
	}
	uo, ok := entries[0].(*upstream.Object)
	if !ok {
		return nil, fs.ErrorNotAFile
	}
	do := uo.UpstreamFs().Features().OpenRangeWriter
	if do == nil {
		return nil, fmt.Errorf("%v cannot write at an offset: %w", uo.UpstreamFs(), fs.ErrorPermissionDenied)
	}
	return do(ctx, uo.UnWrap().Remote())
}

// Check the interface is satisfied
var _ fs.OpenRangeWriter = (*Fs)(nil)
