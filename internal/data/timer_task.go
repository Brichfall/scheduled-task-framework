package data

import (
	"context"

	"github.com/BitofferHub/xtimer/internal/biz"
	"github.com/BitofferHub/xtimer/internal/constant"
	"gorm.io/gorm/clause"
)

type timerTaskRepo struct {
	data *Data
}

func NewTimerTaskRepo(data *Data) biz.TimerTaskRepo {
	return &timerTaskRepo{
		data: data,
	}
}

func (r *timerTaskRepo) BatchSave(ctx context.Context, g []*biz.TimerTask) error {
	// 开启事务的话, 需要调用r.data.DB(ctx) 而不是r.data.db
	err := r.data.DB(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "timer_id"}, {Name: "run_timer"}},
		DoUpdates: clause.AssignmentColumns([]string{}),
	}).Create(g).Error
	return err
}

func (r *timerTaskRepo) TryClaim(ctx context.Context, timerID int64, runTimer int64) (bool, error) {
	result := r.data.db.WithContext(ctx).
		Model(&biz.TimerTask{}).
		Where("timer_id = ?", timerID).
		Where("run_timer = ?", runTimer).
		Where("status = ?", constant.NotRunned.ToInt()).
		Update("status", constant.Running.ToInt())
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == 1, nil
}

func (r *timerTaskRepo) Update(ctx context.Context, g *biz.TimerTask) (*biz.TimerTask, error) {
	err := r.data.db.WithContext(ctx).Where("task_id = ?", g.TaskID).Updates(g).Error
	return g, err
}

func (r *timerTaskRepo) GetTasksByTimeRange(ctx context.Context, startTime int64, endTime int64, status int) ([]*biz.TimerTask, error) {
	var tasks []*biz.TimerTask
	err := r.data.db.WithContext(ctx).
		Where("run_timer >= ?", startTime).
		Where("run_timer <= ?", endTime).
		Where("status = ?", status).
		Find(&tasks).Error
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (r *timerTaskRepo) GetTasksByTimerIdAndRunTimer(ctx context.Context, timerId int64, runTimer int64) (*biz.TimerTask, error) {
	var task *biz.TimerTask
	err := r.data.db.WithContext(ctx).
		Where("timer_id = ?", timerId).
		Where("run_timer = ?", runTimer).
		First(&task).Error
	if err != nil {
		return nil, err
	}
	return task, nil
}
