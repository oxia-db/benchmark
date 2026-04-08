/*
 * Copyright 2025 The Oxia Authors
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

package io.oxia.benchmark.runner.sequence;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates Zipf-distributed values using the rejection-inversion method. This matches the
 * algorithm used by Go's math/rand.Zipf (Hormann & Derflinger).
 */
final class ZipfGenerator implements SequenceGenerator {

  private final double exponent;
  private final long imax;
  private final double hxm;
  private final double h0x5;
  private final double s;

  ZipfGenerator(long maxSequence) {
    this(maxSequence, 1.1);
  }

  ZipfGenerator(long maxSequence, double exponent) {
    this.exponent = exponent;
    this.imax = maxSequence - 1;
    double v = 1.0;
    double q = exponent;

    this.h0x5 = h(0.5);
    this.hxm = h((double) imax + 0.5);
    this.s = 1 - hInv(h(1.5) - Math.exp(-q * Math.log(v + 1)));
  }

  @Override
  public long next() {
    ThreadLocalRandom rng = ThreadLocalRandom.current();
    while (true) {
      double u = h0x5 + rng.nextDouble() * (hxm - h0x5);
      double x = hInv(u);
      long k = (long) (x + 0.5);
      if (k < 0) k = 0;
      if (k > imax) k = imax;
      if (k - x <= s || u >= h(k + 0.5) - Math.exp(-Math.log(k + 1) * exponent)) {
        return k;
      }
    }
  }

  private double h(double x) {
    return -Math.expm1(-exponent * Math.log(x + 1)) / exponent;
  }

  private double hInv(double x) {
    return Math.expm1(-Math.log1p(-x * exponent) / exponent);
  }
}
