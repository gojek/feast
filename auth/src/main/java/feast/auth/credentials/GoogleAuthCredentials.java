/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
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
package feast.auth.credentials;

import static io.grpc.Metadata.ASCII_STRING_MARSHALLER;

import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Status;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Executor;

/*
 * Google auth provider's callCredentials Implementation for serving.
 * Used by CoreSpecService to connect to core.
 */
public class GoogleAuthCredentials extends CallCredentials {
  private String accessToken;
  private Instant tokenExpiryTime;
  private final IdTokenCredentials credentials;
  private static final String BEARER_TYPE = "Bearer";
  private static final Metadata.Key<String> AUTHORIZATION_METADATA_KEY =
      Metadata.Key.of("Authorization", ASCII_STRING_MARSHALLER);

  public GoogleAuthCredentials(Map<String, String> options) throws IOException {

    String targetAudience = options.getOrDefault("audience", "https://localhost");
    ServiceAccountCredentials serviceCreds =
        (ServiceAccountCredentials)
            ServiceAccountCredentials.getApplicationDefault()
                .createScoped(Arrays.asList("openid", "email"));

    credentials =
        IdTokenCredentials.newBuilder()
            .setIdTokenProvider(serviceCreds)
            .setTargetAudience(targetAudience)
            .build();
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
    appExecutor.execute(
        () -> {
          try {
            // Fetches new token if it is not available or if token has expired.
            if (this.accessToken == null || Instant.now().isAfter(this.tokenExpiryTime)) {
              credentials.refreshIfExpired();
              this.accessToken = credentials.getIdToken().getTokenValue();
              this.tokenExpiryTime = credentials.getIdToken().getExpirationTime().toInstant();
            }
            Metadata headers = new Metadata();
            headers.put(
                AUTHORIZATION_METADATA_KEY, String.format("%s %s", BEARER_TYPE, this.accessToken));
            applier.apply(headers);
          } catch (Throwable e) {
            applier.fail(Status.UNAUTHENTICATED.withCause(e));
          }
        });
  }

  @Override
  public void thisUsesUnstableApi() {
    // TODO Auto-generated method stub

  }
}
