/*
 * Copyright (c) 2018 VMware Inc. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hillview.utils;

import org.hillview.dataset.api.Empty;
import org.hillview.dataset.api.IMap;

import java.util.ArrayList;
import java.util.List;

/**
 * This class can be used to create a dataset that has 'cores' times more
 * parallelism than an initial one.
 */
public class Parallelizer implements IMap<Empty, List<Empty>> {
    private final int cores;

    public Parallelizer(int cores) {
        this.cores = cores;
    }

    @Override
    public List<Empty> apply(Empty data) {
        List<Empty> result = new ArrayList<Empty>();
        for (int i = 0; i < cores; i++)
        result.add(new Empty());
        return result;
    }
}
