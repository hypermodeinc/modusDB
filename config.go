/*
 * Copyright 2025 Hypermode Inc.
 * Licensed under the terms of the Apache License, Version 2.0
 * See the LICENSE file that accompanied this code for further details.
 *
 * SPDX-FileCopyrightText: 2025 Hypermode Inc. <hello@hypermode.com>
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

func (cc Config) WithLimitNormalizeNode(n int) Config {
	cc.limitNormalizeNode = n
	return cc
}

func (cc Config) validate() error {
	if cc.dataDir == "" {
		return ErrEmptyDataDir
	}

	return nil
}
