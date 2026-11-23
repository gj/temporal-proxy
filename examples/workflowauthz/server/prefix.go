package main

import (
	"context"
	"strings"

	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
	"go.temporal.io/proxy/pkg/ext"
)

// A team owns the workflow IDs and schedule IDs that begin with "<team>/".
type prefixPolicy struct{}

func (prefixPolicy) workflows(_ context.Context, team, _ string, ops []*authv1.WorkflowOperation) (*authv1.AuthResponse, error) {
	if len(ops) == 0 {
		return ext.Deny("the request lists no workflows"), nil
	}

	for _, op := range ops {
		if !ownsID(team, op.GetWorkflowId()) {
			return ext.Deny("workflow ID " + op.GetWorkflowId() + " is not under " + team + "/"), nil
		}
		if op.GetStart() != nil && !ownsType(team, op.GetStart().GetWorkflowType()) {
			return ext.Deny("workflow type " + op.GetStart().GetWorkflowType() + " is not one of " + team + "'s"), nil
		}
	}

	return ext.Allow(), nil
}

func (p prefixPolicy) schedule(ctx context.Context, team, namespace string, schedule *authv1.ScheduleOperation) (*authv1.AuthResponse, error) {
	if !ownsID(team, schedule.GetScheduleId()) {
		return ext.Deny("schedule ID " + schedule.GetScheduleId() + " is not under " + team + "/"), nil
	}

	// A request that creates or updates the schedule sets the workflow it starts.
	if action := schedule.GetAction(); action != nil {
		return p.workflows(ctx, team, namespace, []*authv1.WorkflowOperation{action})
	}

	return ext.Allow(), nil
}

func ownsID(team, id string) bool {
	return strings.HasPrefix(id, team+"/")
}
