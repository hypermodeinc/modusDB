/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package modusdb

type Config struct {
	dataDir string

	// optional params
	limitNormalizeNode int
}

func NewDefaultConfig(dir string) Config {
	return Config{dataDir: dir, limitNormalizeNode: 10000}
}

func (cc Config) WithLimitNormalizeNode(d int) Config {
	cc.limitNormalizeNode = d
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
