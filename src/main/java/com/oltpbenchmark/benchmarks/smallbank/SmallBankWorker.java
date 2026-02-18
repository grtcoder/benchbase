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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallbank.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.DiscreteRNG;
import com.oltpbenchmark.util.RandomDistribution.Flat;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SmallBank Benchmark Work Driver
 *
 * @author pavlo
 */
public final class SmallBankWorker extends Worker<SmallBankBenchmark> {
  private static final Logger LOG = LoggerFactory.getLogger(SmallBankWorker.class);

  private final Amalgamate procAmalgamate;
  private final Balance procBalance;
  private final DepositChecking procDepositChecking;
  private final SendPayment procSendPayment;
  private final TransactSavings procTransactSavings;
  private final WriteCheck procWriteCheck;

  private final SmallBankRestClient restClient;
  private final DiscreteRNG rng;
  private final long numAccounts;
  private final long[] custIdsBuffer = {-1L, -1L};

  public SmallBankWorker(SmallBankBenchmark benchmarkModule, int id) {
    super(benchmarkModule, id);

    this.procAmalgamate = this.getProcedure(Amalgamate.class);
    this.procBalance = this.getProcedure(Balance.class);
    this.procDepositChecking = this.getProcedure(DepositChecking.class);
    this.procSendPayment = this.getProcedure(SendPayment.class);
    this.procTransactSavings = this.getProcedure(TransactSavings.class);
    this.procWriteCheck = this.getProcedure(WriteCheck.class);

    this.numAccounts = benchmarkModule.numAccounts;
    this.restClient = benchmarkModule.getRestClient();
    this.rng = new Flat(rng(), 0, this.numAccounts);
  }

  protected void generateCustIds(boolean needsTwoAccts) {
    for (int i = 0; i < this.custIdsBuffer.length; i++) {
      this.custIdsBuffer[i] = this.rng.nextLong();

      // They can never be the same!
      if (i > 0 && this.custIdsBuffer[i - 1] == this.custIdsBuffer[i]) {
        i--;
        continue;
      }

      // If we only need one acctId, break out here.
      if (i == 0 && !needsTwoAccts) {
        break;
      }
      // If we need two acctIds, then we need to go generate the second one
      if (i == 0) {
        continue;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Accounts: %s", Arrays.toString(this.custIdsBuffer)));
    }
  }

  @Override
  protected TransactionStatus executeWork(Connection conn, TransactionType txnType)
      throws UserAbortException, SQLException {
    Class<? extends Procedure> procClass = txnType.getProcedureClass();

    try {
      // Amalgamate
      if (procClass.equals(Amalgamate.class)) {
        this.generateCustIds(true);
        this.procAmalgamate.run(this.restClient, this.custIdsBuffer[0], this.custIdsBuffer[1]);

        // Balance
      } else if (procClass.equals(Balance.class)) {
        this.generateCustIds(false);
        this.procBalance.run(this.restClient, this.custIdsBuffer[0]);

        // DepositChecking
      } else if (procClass.equals(DepositChecking.class)) {
        this.generateCustIds(false);
        this.procDepositChecking.run(
            this.restClient,
            this.custIdsBuffer[0],
            SmallBankConstants.PARAM_DEPOSIT_CHECKING_AMOUNT);

        // SendPayment
      } else if (procClass.equals(SendPayment.class)) {
        this.generateCustIds(true);
        this.procSendPayment.run(
            this.restClient,
            this.custIdsBuffer[0],
            this.custIdsBuffer[1],
            SmallBankConstants.PARAM_SEND_PAYMENT_AMOUNT);

        // TransactSavings
      } else if (procClass.equals(TransactSavings.class)) {
        this.generateCustIds(false);
        this.procTransactSavings.run(
            this.restClient,
            this.custIdsBuffer[0],
            SmallBankConstants.PARAM_TRANSACT_SAVINGS_AMOUNT);

        // WriteCheck
      } else if (procClass.equals(WriteCheck.class)) {
        this.generateCustIds(false);
        this.procWriteCheck.run(
            this.restClient, this.custIdsBuffer[0], SmallBankConstants.PARAM_WRITE_CHECK_AMOUNT);
      }
    } catch (IOException e) {
      LOG.error("REST call failed", e);
      throw new SQLException("REST call failed: " + e.getMessage(), e);
    }

    return TransactionStatus.SUCCESS;
  }
}
