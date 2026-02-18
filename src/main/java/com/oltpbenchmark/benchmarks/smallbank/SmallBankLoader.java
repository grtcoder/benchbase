/*
 * Copyright 2020 by OLTPBenchmark Project
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
 *
 */

package com.oltpbenchmark.benchmarks.smallbank;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.smallbank.SmallBankRestClient.Operation;
import com.oltpbenchmark.util.RandomDistribution.DiscreteRNG;
import com.oltpbenchmark.util.RandomDistribution.Gaussian;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * SmallBankBenchmark Loader - loads data via REST calls to the broker.
 *
 * @author pavlo
 */
public final class SmallBankLoader extends Loader<SmallBankBenchmark> {

  private final long numAccounts;

  public SmallBankLoader(SmallBankBenchmark benchmark) {
    super(benchmark);
    this.numAccounts = benchmark.numAccounts;
  }

  @Override
  public List<LoaderThread> createLoaderThreads() throws SQLException {
    List<LoaderThread> threads = new ArrayList<>();
    int batchSize = 100000;
    long start = 0;
    while (start < this.numAccounts) {
      long stop = Math.min(start + batchSize, this.numAccounts);
      threads.add(new Generator(start, stop));
      start = stop;
    }
    return (threads);
  }

  /** Thread that can generate a range of accounts */
  private class Generator extends LoaderThread {
    private final long start;
    private final long stop;
    private final DiscreteRNG randBalance;

    public Generator(long start, long stop) {
      super(benchmark);
      this.start = start;
      this.stop = stop;
      this.randBalance =
          new Gaussian(
              benchmark.rng(), SmallBankConstants.MIN_BALANCE, SmallBankConstants.MAX_BALANCE);
    }

    @Override
    public void load(Connection conn) {
      SmallBankRestClient client = SmallBankLoader.this.benchmark.getRestClient();

      try {
        for (long acctId = this.start; acctId < this.stop; acctId++) {
          String acctName = String.valueOf(acctId);
          int savingsBalance = this.randBalance.nextInt();
          int checkingBalance = this.randBalance.nextInt();

          client.sendTransaction(
              SmallBankRestClient.TX_LOAD_CUSTOMER,
              Arrays.asList(
                  new Operation(
                      SmallBankRestClient.accountKey(acctId),
                      acctName,
                      SmallBankRestClient.OP_WRITE),
                  new Operation(
                      SmallBankRestClient.savingsKey(acctId),
                      Integer.toString(savingsBalance),
                      SmallBankRestClient.OP_WRITE),
                  new Operation(
                      SmallBankRestClient.checkingKey(acctId),
                      Integer.toString(checkingBalance),
                      SmallBankRestClient.OP_WRITE)));
        }
      } catch (IOException ex) {
        LOG.error("Failed to load data via REST", ex);
        throw new RuntimeException(ex);
      }
    }
  }
}
