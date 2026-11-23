package biz

import (
	"context"
	"time"

	"github.com/BitofferHub/pkg/middlewares/lock"
	"github.com/BitofferHub/pkg/middlewares/log"
	"github.com/BitofferHub/xtimer/internal/conf"
	"github.com/BitofferHub/xtimer/internal/utils"
)

// xtimerUseCase is a User usecase.
type SchedulerUseCase struct {
	confData  *conf.Data
	timerRepo TimerRepo
	taskRepo  TimerTaskRepo
	taskCache TaskCache
	tm        Transaction
	pool      WorkerPool
	trigger   *TriggerUseCase
}

// NewUserUseCase new a User usecase.
func NewSchedulerUseCase(confData *conf.Data, timerRepo TimerRepo, taskRepo TimerTaskRepo, taskCache TaskCache, trigger *TriggerUseCase) *SchedulerUseCase {
	return &SchedulerUseCase{
		confData:  confData,
		timerRepo: timerRepo,
		taskRepo:  taskRepo,
		taskCache: taskCache,
		pool:      NewGoWorkerPool(int(confData.Scheduler.WorkersNum)),
		trigger:   trigger,
	}
}

func (w *SchedulerUseCase) Work(ctx context.Context) error {

	ticker := time.NewTicker(time.Duration(w.confData.Scheduler.TryLockGapMilliSeconds) * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-ctx.Done():
			log.WarnContextf(ctx, "stopped")
			return nil
		default:
		}

		w.handleSlices(ctx)
	}
	return nil
}

func (w *SchedulerUseCase) handleSlices(ctx context.Context) {
	for i := 0; i < w.getValidBucket(ctx); i++ {
		w.handleSlice(ctx, i)
	}
}

// 禁用动态分桶能力
func (w *SchedulerUseCase) getValidBucket(ctx context.Context) int {
	return int(w.confData.Scheduler.BucketsNum)
}

func (w *SchedulerUseCase) handleSlice(ctx context.Context, bucketID int) {
	defer func() {
		// 捕获异常，不再上报
		if r := recover(); r != nil {
			log.ErrorContextf(ctx, "handleSlice %v run err. Recovered from panic:%v", bucketID, r)
		}
	}()
	log.InfoContextf(ctx, "scheduler_%v start: %v", bucketID, time.Now())
	now := time.Now()
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now.Add(-time.Minute), bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed, err: %v", err)
	}
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now, bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed, err: %v", err)
	}
	log.InfoContextf(ctx, "scheduler_%v end: %v", bucketID, time.Now())
}

func (w *SchedulerUseCase) asyncHandleSlice(ctx context.Context, t time.Time, bucketID int) {
	// 限制激活和去激活频次
	//每次获取锁都是创建一个新的锁对象
	lockTTL := w.confData.Scheduler.SuccessExpireSeconds
	if lockTTL <= 0 {
		lockTTL = w.confData.Scheduler.TryLockSeconds
	}
	if lockTTL <= 0 {
		log.ErrorContextf(ctx, "invalid scheduler lock ttl")
		return
	}

	locker := lock.NewRedisLock(utils.GetTimeBucketLockKey(t, bucketID),
		lock.WithExpireSeconds(lockTTL))
	err := locker.Lock(ctx)
	if err != nil {
		//log.InfoContextf(ctx, "asyncHandleSlice 获取分布式锁失败: %v", err.Error())
		// 抢锁失败, 直接跳过执行, 下一轮
		return
	}

	log.InfoContextf(ctx, "get scheduler lock success, key: %s", utils.GetTimeBucketLockKey(t, bucketID))

	success := false
	defer func() {
		if success {
			return
		}

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if unlockErr := locker.Unlock(cleanupCtx); unlockErr != nil {
			log.ErrorContextf(ctx, "unlock trigger lock failed, lock key: %s, err: %v", utils.GetTimeBucketLockKey(t, bucketID), unlockErr)
		}
	}()

	// 固定 TTL 在抢锁时已设置，成功后不需要再修改锁的过期时间。
	ack := func() {}
	//到trigger的时候已经是格式化的分钟的格式
	if err := w.trigger.Work(ctx, utils.GetSliceMsgKey(t, bucketID), ack); err != nil {
		log.ErrorContextf(ctx, "trigger work failed, SliceMsgKey[%v] err: %v", utils.GetSliceMsgKey(t, bucketID), err)
		return
	}

	success = true
}
