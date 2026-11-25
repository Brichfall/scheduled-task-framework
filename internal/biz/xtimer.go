package biz

import (
	"context"
	"fmt"
	"time"

	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/constant"
)

// 表定义也需要放在biz层, 还是为了解耦biz与data层
type Timer struct {
	TimerId         int64 `gorm:"column:id"`
	App             string
	Name            string
	Status          int
	Cron            string
	NotifyHTTPParam string     `gorm:"column:notify_http_param;NOT NULL" json:"notify_http_param,omitempty"` // Http 回调参数
	CreateTime      *time.Time `gorm:"column:create_time;default:null"`
	ModifyTime      *time.Time `gorm:"column:modify_time;default:null"`
}

// TableName 表名
func (p *Timer) TableName() string {
	return "xtimer"
}

func (t *Timer) BatchTasksFromTimer(executeTimes []time.Time) []*TimerTask {
	//将对应的执行时间的切片转化成
	tasks := make([]*TimerTask, 0, len(executeTimes))
	for _, executeTime := range executeTimes {
		tasks = append(tasks, &TimerTask{
			App:      t.App,
			TimerID:  t.TimerId,
			Status:   constant.NotRunned.ToInt(),
			RunTimer: executeTime.UnixMilli(),
		})
	}
	return tasks
}

// TimerRepo is a Greater timerRepo.
type TimerRepo interface {
	Save(context.Context, *Timer) (*Timer, error)
	UpdateStatus(ctx context.Context, app string, timerID int64, from, to constant.TimerStatus) (bool, error)
	FindByID(context.Context, int64) (*Timer, error)
	FindByStatus(context.Context, int) ([]*Timer, error)
	Delete(context.Context, int64) error
}

// xtimerUseCase is a User usecase.
type XtimerUseCase struct {
	confData  *conf.Data
	timerRepo TimerRepo
	taskRepo  TimerTaskRepo
	taskCache TaskCache
	tm        Transaction
	muc       *MigratorUseCase
}

// NewUserUseCase new a User usecase.
func NewXtimerUseCase(confData *conf.Data, timerRepo TimerRepo, taskRepo TimerTaskRepo, taskCache TaskCache, tm Transaction, muc *MigratorUseCase) *XtimerUseCase {
	return &XtimerUseCase{confData: confData, timerRepo: timerRepo, taskRepo: taskRepo, taskCache: taskCache, tm: tm, muc: muc}
}

func (uc *XtimerUseCase) CreateTimer(ctx context.Context, g *Timer) (*Timer, error) {
	return uc.timerRepo.Save(ctx, g)
}

func (uc *XtimerUseCase) EnableTimer(ctx context.Context, app string, timerID int64) error {
	return uc.tm.InTx(ctx, func(ctx context.Context) error {
		updated, err := uc.timerRepo.UpdateStatus(ctx, app, timerID, constant.Unabled, constant.Enabled)
		if err != nil {
			return fmt.Errorf("激活 Timer %d 失败: %w", timerID, err)
		}
		if !updated {
			return fmt.Errorf("应用 %q 下没有可激活的 Timer %d", app, timerID)
		}

		timer, err := uc.timerRepo.FindByID(ctx, timerID)
		if err != nil {
			return fmt.Errorf("查询 Timer %d 失败: %w", timerID, err)
		}

		// 迁移数据，这里的迁移只涉及到对应的一个timer
		if err := uc.muc.MigratorTimer(ctx, timer); err != nil {
			log.ErrorContextf(ctx, "迁移timer失败: %v", err)
			return err
		}
		return nil
	})
}

func (uc *XtimerUseCase) DisableTimer(ctx context.Context, app string, timerID int64) error {
	updated, err := uc.timerRepo.UpdateStatus(ctx, app, timerID, constant.Enabled, constant.Unabled)
	if err != nil {
		return fmt.Errorf("停用 Timer %d 失败: %w", timerID, err)
	}
	if updated {
		return nil
	}

	// 已停用的 Timer 重复停用时返回成功。
	timer, err := uc.timerRepo.FindByID(ctx, timerID)
	if err != nil {
		return fmt.Errorf("查询 Timer %d 失败: %w", timerID, err)
	}
	if timer.App != app {
		return fmt.Errorf("Timer %d 不属于应用 %q", timerID, app)
	}
	if timer.Status == constant.Unabled.ToInt() {
		return nil
	}
	return fmt.Errorf("Timer %d 的状态未更新", timerID)
}
