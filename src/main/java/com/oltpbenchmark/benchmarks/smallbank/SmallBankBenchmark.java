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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.smallbank.procedures.Amalgamate;
import com.oltpbenchmark.catalog.Column;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;
import java.util.ArrayList;
import java.util.List;

public final class SmallBankBenchmark extends BenchmarkModule {

  protected final long numAccounts;
  private final List<String> brokerUrls;

  public SmallBankBenchmark(WorkloadConfiguration workConf) {
    super(workConf);
    this.numAccounts =
        (int) Math.round(SmallBankConstants.NUM_ACCOUNTS * workConf.getScaleFactor());

    List<String> urls = new ArrayList<>();
    if (workConf.getXmlConfig() != null) {
      if (workConf.getXmlConfig().containsKey("brokers/broker[1]/host")) {
        int i = 1;
        while (workConf.getXmlConfig().containsKey("brokers/broker[" + i + "]/host")) {
          String host = workConf.getXmlConfig().getString("brokers/broker[" + i + "]/host");
          int port = workConf.getXmlConfig().getInt("brokers/broker[" + i + "]/port");
          urls.add("http://" + host + ":" + port + "/addTransaction");
          i++;
        }
      } else {
        String host = workConf.getXmlConfig().getString("brokerHost", "127.0.0.1");
        int port = workConf.getXmlConfig().getInt("brokerPort", 8090);
        urls.add("http://" + host + ":" + port + "/addTransaction");
      }
    } else {
      urls.add("http://127.0.0.1:8090/addTransaction");
    }
    this.brokerUrls = urls;
  }

  public SmallBankRestClient newRestClient() {
    return new SmallBankRestClient(brokerUrls);
  }

  @Override
  protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
    List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
    for (int i = 0; i < workConf.getTerminals(); ++i) {
      workers.add(new SmallBankWorker(this, i));
    }
    return workers;
  }

  @Override
  protected Loader<SmallBankBenchmark> makeLoaderImpl() {
    return new SmallBankLoader(this);
  }

  @Override
  protected Package getProcedurePackageImpl() {
    return Amalgamate.class.getPackage();
  }

  /**
   * For the given table, return the length of the first VARCHAR attribute
   *
   * @param acctsTbl
   * @return
   */
  public static int getCustomerNameLength(Table acctsTbl) {
    int acctNameLength = -1;
    for (Column col : acctsTbl.getColumns()) {
      if (SQLUtil.isStringType(col.getType())) {
        acctNameLength = col.getSize();
        break;
      }
    }

    return (acctNameLength);
  }
}
