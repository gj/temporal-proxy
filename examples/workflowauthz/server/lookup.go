package main

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
	"go.temporal.io/proxy/pkg/ext"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Asks the Temporal Service for the workflow type behind an ID, so IDs need no
// convention. The answer can be out of date when the request arrives: see "The
// race in the lookup approach" in the README.
type lookupPolicy struct {
	temporal workflowservice.WorkflowServiceClient
}

func (p lookupPolicy) workflows(ctx context.Context, team, namespace string, ops []*authv1.WorkflowOperation) (*authv1.AuthResponse, error) {
	if len(ops) == 0 {
		return ext.Deny("the request lists no workflows"), nil
	}

	for _, op := range ops {
		if op.GetStart() != nil && !ownsType(team, op.GetStart().GetWorkflowType()) {
			return ext.Deny("workflow type " + op.GetStart().GetWorkflowType() + " is not one of " + team + "'s"), nil
		}

		// A request that starts a workflow can also signal or terminate one that
		// already runs under the same ID, so that workflow must be the team's too.
		// The latest workflow under the ID counts even after it closes.
		latest, err := p.latestWorkflowType(ctx, namespace, op.GetWorkflowId())
		if err != nil {
			return nil, err
		}
		if latest == "" && op.GetStart() == nil {
			return ext.Deny("no workflow has ID " + op.GetWorkflowId()), nil
		}
		if latest != "" && !ownsType(team, latest) {
			return ext.Deny("workflow " + op.GetWorkflowId() + " has type " + latest + ", which is not one of " + team + "'s"), nil
		}
	}

	return ext.Allow(), nil
}

func (p lookupPolicy) schedule(ctx context.Context, team, namespace string, schedule *authv1.ScheduleOperation) (*authv1.AuthResponse, error) {
	// A request that creates or updates the schedule sets the workflow it starts.
	action := schedule.GetAction()
	if action != nil && !ownsType(team, action.GetStart().GetWorkflowType()) {
		return ext.Deny("workflow type " + action.GetStart().GetWorkflowType() + " is not one of " + team + "'s"), nil
	}

	scheduled, err := p.scheduledWorkflowType(ctx, namespace, schedule.GetScheduleId())
	if err != nil {
		return nil, err
	}
	if scheduled == "" && action == nil {
		return ext.Deny("no schedule has ID " + schedule.GetScheduleId()), nil
	}
	if scheduled != "" && !ownsType(team, scheduled) {
		return ext.Deny("schedule " + schedule.GetScheduleId() + " starts " + scheduled + ", which is not one of " + team + "'s"), nil
	}

	return ext.Allow(), nil
}

// The type of the latest workflow under the ID, running or closed. Returns ""
// when no workflow has that ID.
func (p lookupPolicy) latestWorkflowType(ctx context.Context, namespace, workflowID string) (string, error) {
	resp, err := p.temporal.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: namespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	if status.Code(err) == codes.NotFound {
		return "", nil
	}

	return resp.GetWorkflowExecutionInfo().GetType().GetName(), err
}

// Returns "" when no schedule has that ID.
func (p lookupPolicy) scheduledWorkflowType(ctx context.Context, namespace, scheduleID string) (string, error) {
	resp, err := p.temporal.DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  namespace,
		ScheduleId: scheduleID,
	})
	if status.Code(err) == codes.NotFound {
		return "", nil
	}

	return resp.GetSchedule().GetAction().GetStartWorkflow().GetWorkflowType().GetName(), err
}
