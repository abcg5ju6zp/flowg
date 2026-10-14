package operations

import (
	"context"
	"log/slog"

	"net/http"

	"go.uber.org/fx"

	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"

	"link-society.com/flowg/api/auth"
	"link-society.com/flowg/api/routing"
	"link-society.com/flowg/api/schemas"

	"link-society.com/flowg/internal/engines/pipelines"
	"link-society.com/flowg/internal/models"
	storage "link-society.com/flowg/internal/storage/interfaces"
)

// GetPipelineStatusesDeps lists the dependencies of
// [NewGetPipelineStatusesUsecase].
type GetPipelineStatusesDeps struct {
	fx.In

	Logger *slog.Logger

	AuthStorage     storage.AuthStorage
	PipelinesRunner pipelines.Runner
}

// NewGetPipelineStatusesUsecase returns the convergence status of every
// pipeline: the configuration version currently serving, the desired version
// and the running/pending/error phase, including failures hidden behind a last
// known good build.
//
// Callers must have the read-pipelines permission.
func NewGetPipelineStatusesUsecase(deps GetPipelineStatusesDeps) usecase.Interactor {
	u := usecase.NewInteractor(
		auth.RequireScopeApiDecorator(
			deps.Logger,
			deps.AuthStorage,
			models.SCOPE_READ_PIPELINES,
			func(
				ctx context.Context,
				req schemas.GetPipelineStatusesRequest,
				resp *schemas.GetPipelineStatusesResponse,
			) error {
				statuses, err := deps.PipelinesRunner.Status(ctx)
				if err != nil {
					deps.Logger.ErrorContext(
						ctx,
						"Failed to query pipeline statuses",
						slog.String("error", err.Error()),
					)

					resp.Success = false
					return status.Wrap(err, status.Internal)
				}

				resp.Success = true
				resp.Statuses = make([]schemas.PipelineStatus, 0, len(statuses))
				for _, pipelineStatus := range statuses {
					resp.Statuses = append(resp.Statuses, schemas.PipelineStatus{
						Pipeline:       pipelineStatus.Name,
						Phase:          string(pipelineStatus.Phase),
						ActiveVersion:  pipelineStatus.ActiveVersion,
						DesiredVersion: pipelineStatus.DesiredVersion,
						Serving:        pipelineStatus.Serving,
						Error:          pipelineStatus.Error,
					})
				}

				return nil
			},
		),
	)

	u.SetName("get_pipeline_statuses")
	u.SetTitle("Get Pipeline Statuses")
	u.SetDescription("Get the configuration version and convergence status of every pipeline")
	u.SetTags("pipelines")

	u.SetExpectedErrors(status.PermissionDenied, status.Internal)

	return u
}

func init() {
	routing.RegisterOperation(
		NewGetPipelineStatusesUsecase,
		http.MethodGet,
		"/api/v1/status/pipelines",
	)
}
