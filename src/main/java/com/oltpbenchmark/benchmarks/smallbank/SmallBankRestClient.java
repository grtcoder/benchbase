package com.oltpbenchmark.benchmarks.smallbank;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST client for sending SmallBank transactions to the Go broker's /addTransaction endpoint. */
public class SmallBankRestClient {
  private static final Logger LOG = LoggerFactory.getLogger(SmallBankRestClient.class);

  // SmallBank transaction type constants (must match Go commons.go)
  public static final int TX_NORMAL = 0;
  public static final int TX_AMALGAMATE = 1;
  public static final int TX_BALANCE = 2;
  public static final int TX_DEPOSIT_CHECK = 3;
  public static final int TX_SEND_PAYMENT = 4;
  public static final int TX_TRANSACT_SAV = 5;
  public static final int TX_WRITE_CHECK = 6;
  public static final int TX_LOAD_CUSTOMER = 7;

  // Operation types (must match Go commons.go)
  public static final int OP_WRITE = 1;
  public static final int OP_DELETE = 2;
  public static final int OP_READ = 3;

  // Key type suffix bytes
  private static final byte KEY_ACCOUNTS = 0x01;
  private static final byte KEY_SAVINGS = 0x02;
  private static final byte KEY_CHECKING = 0x03;

  private final List<String> brokerUrls;
  private final HttpClient httpClient;
  private final AtomicLong idCounter = new AtomicLong(0);

  public SmallBankRestClient(List<String> brokerUrls) {
    this.brokerUrls = brokerUrls;
    this.httpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
  }

  /** Send a transaction to the broker. */
  public void sendTransaction(int transactionType, List<Operation> operations) throws IOException {
    long id = idCounter.incrementAndGet();
    long assignedTs = 0;
    StringBuilder json = new StringBuilder();
    json.append("{\"id\":").append(id);
    json.append(",\"timestamp\":{\"EpochID\":0,\"BrokerID\":0,\"AssignedTs\":")
        .append(assignedTs)
        .append("}");
    json.append(",\"TransactionType\":").append(transactionType);
    json.append(",\"operations\":[");
    for (int i = 0; i < operations.size(); i++) {
      if (i > 0) json.append(",");
      Operation op = operations.get(i);
      json.append("{\"key\":\"").append(escapeJson(op.key)).append("\"");
      json.append(",\"value\":\"").append(escapeJson(op.value)).append("\"");
      json.append(",\"op\":").append(op.op).append("}");
    }
    json.append("]}");

    String brokerUrl = brokerUrls.get(ThreadLocalRandom.current().nextInt(brokerUrls.size()));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(brokerUrl))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json.toString()))
            .build();

    try {
      HttpResponse<Void> response =
          httpClient.send(request, HttpResponse.BodyHandlers.discarding());
      if (response.statusCode() != 200) {
        LOG.warn("Broker returned status {}", response.statusCode());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while sending transaction", e);
    }
  }

  // Key format helpers: 8-byte little-endian custId + 1-byte type suffix, base64-encoded
  private static byte[] buildKey(byte type, long custId) {
    byte[] b = new byte[9];
    ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN).putLong(custId);
    b[8] = type;
    return b;
  }

  public static String accountKey(long custId) {
    return Base64.getEncoder().encodeToString(buildKey(KEY_ACCOUNTS, custId));
  }

  public static String savingsKey(long custId) {
    return Base64.getEncoder().encodeToString(buildKey(KEY_SAVINGS, custId));
  }

  public static String checkingKey(long custId) {
    return Base64.getEncoder().encodeToString(buildKey(KEY_CHECKING, custId));
  }

  private static String escapeJson(String s) {
    if (s == null) return "";
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  /** Represents a single operation within a transaction. */
  public static class Operation {
    public final String key;
    public final String value;
    public final int op;

    public Operation(String key, String value, int op) {
      this.key = key;
      this.value = value;
      this.op = op;
    }
  }
}
