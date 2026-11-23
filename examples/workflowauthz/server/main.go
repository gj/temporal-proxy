// Command server is an extension server that decides each call by the team
// that owns the workflow or the schedule. See the README.
package main

import (
	"context"
	"flag"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
	authv1 "go.temporal.io/proxy/pkg/api/auth/v1"
	"go.temporal.io/proxy/pkg/ext"
	"go.temporal.io/proxy/pkg/logger"
	"go.temporal.io/proxy/pkg/logger/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Decides the two kinds of request that belong to a team.
	policy interface {
		workflows(ctx context.Context, team, namespace string, ops []*authv1.WorkflowOperation) (*authv1.AuthResponse, error)
		schedule(ctx context.Context, team, namespace string, schedule *authv1.ScheduleOperation) (*authv1.AuthResponse, error)
	}

	authorizer struct {
		policy policy
	}
)

func main() {
	listen := flag.String("listen", "127.0.0.1:9444", "address to serve on")
	approach := flag.String("approach", "prefix", `how to find the owner of an ID: "prefix" or "lookup"`)
	temporal := flag.String("temporal", "127.0.0.1:7233", "Temporal Service the lookup approach asks")
	flag.Parse()

	log := logger.Default().With(tag.Component("examples"))

	var p policy
	switch *approach {
	case "prefix":
		p = prefixPolicy{}
	case "lookup":
		cc, err := grpc.NewClient(*temporal, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatal("Failed to create the Temporal client", tag.Error(err))
		}
		p = lookupPolicy{temporal: workflowservice.NewWorkflowServiceClient(cc)}
	default:
		log.Fatal(`-approach must be "prefix" or "lookup"`)
	}

	if err := ext.Serve(
		context.Background(),
		ext.WithAddr(*listen),
		ext.WithAuth(authorizer{policy: p}),
		ext.WithLogger(log),
	); err != nil {
		log.Fatal("Failed to start server", tag.Error(err))
	}
}

func (a authorizer) Authenticate(ctx context.Context, req *authv1.AuthRequest) (*authv1.AuthResponse, error) {
	target := req.GetTarget()
	if ext.IsHealthCheckMethod(target.GetFullName()) {
		return ext.Allow(), nil
	}

	// The team is the bearer token as sent. A real server verifies a credential
	// and reads the team from it, as examples/authz does.
	team, err := ext.BearerToken(req, "authorization")
	if err != nil {
		return nil, err
	}

	switch kind := target.GetWorkflowService().GetKind().(type) {
	case *authv1.WorkflowServiceTarget_Workflows:
		return a.policy.workflows(ctx, team, target.GetNamespace(), kind.Workflows.GetOperations())

	case *authv1.WorkflowServiceTarget_Schedule:
		return a.policy.schedule(ctx, team, target.GetNamespace(), kind.Schedule)

	// A batch over a query does not list its workflows, so neither approach can
	// find their owners.
	case *authv1.WorkflowServiceTarget_VisibilityQuery:
		if team == "admin" {
			return ext.Allow(), nil
		}

		return ext.Deny("only admin may start a batch over a query"), nil
	}

	return ext.Deny("the request is not on a workflow or a schedule"), nil
}

// A team owns the workflow types that begin with "<team>.".
func ownsType(team, workflowType string) bool {
	return strings.HasPrefix(workflowType, team+".")
}
