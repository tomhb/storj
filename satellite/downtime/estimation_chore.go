// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package downtime

import (
	"context"
	"time"

	"go.uber.org/zap"

	"storj.io/common/sync2"
	"storj.io/storj/satellite/overlay"
)

// EstimationChore estimates how long nodes have been offline.
//
// architecture: Chore
type EstimationChore struct {
	log     *zap.Logger
	Loop    *sync2.Cycle
	config  Config
	overlay *overlay.Service
	service *Service
	db      DB
}

// NewEstimationChore instantiates EstimationChore.
func NewEstimationChore(log *zap.Logger, config Config, overlay *overlay.Service, service *Service, db DB) *EstimationChore {
	return &EstimationChore{
		log:     log,
		Loop:    sync2.NewCycle(config.EstimationInterval),
		config:  config,
		overlay: overlay,
		service: service,
		db:      db,
	}
}

// Run starts the chore.
func (chore *EstimationChore) Run(ctx context.Context) (err error) {
	defer mon.Task()(&ctx)(&err)
	return chore.Loop.Run(ctx, func(ctx context.Context) (err error) {
		defer mon.Task()(&ctx)(&err)

		chore.log.Debug("checking uptime of failed nodes",
			zap.Stringer("interval", chore.config.EstimationInterval))

		offlineNodes, err := chore.overlay.GetOfflineNodesLimited(ctx, chore.config.EstimationBatchSize)
		if err != nil {
			chore.log.Error("error getting offline nodes", zap.Error(err))
			return nil
		}

		for _, node := range offlineNodes {
			success, err := chore.service.CheckAndUpdateNodeAvailability(ctx, node.ID, node.Address)
			if err != nil {
				chore.log.Error("error during downtime estimation ping back",
					zap.Bool("success", success),
					zap.Error(err))
				continue
			}
			if !success {
				now := time.Now().UTC()
				duration := now.Sub(node.LastContactFailure)

				err = chore.db.Add(ctx, node.ID, now, duration)
				if err != nil {
					chore.log.Error("error adding node seconds offline information.",
						zap.Stringer("node ID", node.ID),
						zap.Stringer("duration", duration),
						zap.Error(err))
				}
			}
		}
		return nil
	})
}

// Close closes chore.
func (chore *EstimationChore) Close() error {
	chore.Loop.Close()
	return nil
}
