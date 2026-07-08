/*
 * Copyright © 2025 The Oxia Authors
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
package io.oxia.benchmark.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class SessionPoolTest {

    private final MockSessionDriver driver = new MockSessionDriver();

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void rampAttachesEphemeralsAndTracksLiveCount() {
        SessionKeys keys = new SessionKeys("bench/sess");
        SessionPool pool = new SessionPool(driver, keys, 10, Duration.ofSeconds(1), 16);

        pool.rampTo(20, 8);

        assertThat(pool.size()).isEqualTo(20);
        assertThat(pool.liveIds()).hasSize(20);
        // 20 sessions * 10 ephemerals each are present.
        assertThat(driver.presentCount()).isEqualTo(200);
    }

    @Test
    void killRemovesFromPoolAndExpiresKeysAfterTimeout() throws Exception {
        SessionKeys keys = new SessionKeys("bench/sess");
        SessionPool pool = new SessionPool(driver, keys, 1, Duration.ofMillis(200), 16);
        pool.rampTo(10, 8);
        assertThat(driver.presentCount()).isEqualTo(10);

        for (long id : pool.liveIds().subList(0, 4)) {
            pool.kill(id).join();
        }

        // Killed sessions leave the pool immediately, but their keys survive until expiry.
        assertThat(pool.size()).isEqualTo(6);
        assertThat(driver.presentCount()).isEqualTo(10);

        Thread.sleep(400); // let the 200ms mock expiry fire
        assertThat(driver.presentCount()).isEqualTo(6);
    }

    @Test
    void closeRemovesKeysImmediately() {
        SessionKeys keys = new SessionKeys("bench/sess");
        SessionPool pool = new SessionPool(driver, keys, 2, Duration.ofSeconds(5), 16);
        pool.rampTo(5, 4);
        assertThat(driver.presentCount()).isEqualTo(10);

        pool.closeAll(4);

        assertThat(pool.size()).isZero();
        assertThat(driver.presentCount())
                .isZero(); // graceful close is immediate, no waiting for expiry
    }
}
