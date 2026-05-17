package schedule

import (
	"fmt"

	coreerrors "github.com/cntryl/fitz-go/internal/core/errors"
	"github.com/robfig/cron/v3"
)

var scheduleCronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

func validateCronExpression(expr string) error {
	if _, err := scheduleCronParser.Parse(expr); err != nil {
		return coreerrors.NewDomainError(uint32(coreerrors.ScheduleInvalidCron), fmt.Sprintf("invalid cron expression: %v", err))
	}
	return nil
}
