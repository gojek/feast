/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.core.job;

public enum Runner {
  DATAFLOW("DataflowRunner"),
  FLINK("FlinkRunner"),
  DIRECT("DirectRunner");

  private final String name;

  Runner(String name) {
    this.name = name;
  }

  /**
   * Get the human readable name of this runner. Returns a human readable name of the runner that
   * can be used for logging/config files.
   */
  public String getName() {
    return name;
  }

  /** Parses a runner from its human readable name. */
  public static Runner fromString(String runner) {
    for (Runner r : Runner.values()) {
      if (r.getName().equals(runner)) {
        return r;
      }
    }
    throw new IllegalArgumentException("Unknown value: " + runner);
  }
}
