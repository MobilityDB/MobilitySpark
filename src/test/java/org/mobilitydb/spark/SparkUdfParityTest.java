/*****************************************************************************
 *
 * This MobilityDB code is provided under The PostgreSQL License.
 * Copyright (c) 2020-2026, Université libre de Bruxelles and MobilityDB
 * contributors
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written
 * agreement is hereby granted, provided that the above copyright notice and
 * this paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL UNIVERSITE LIBRE DE BRUXELLES BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
 * EVEN IF UNIVERSITE LIBRE DE BRUXELLES HAS BEEN ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 * UNIVERSITE LIBRE DE BRUXELLES SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON
 * AN "AS IS" BASIS, AND UNIVERSITE LIBRE DE BRUXELLES HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 *****************************************************************************/

package org.mobilitydb.spark;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Honest, test-enforced Spark UDF parity against the MobilityDB SQL surface.
 *
 * <p>Mirrors the ecosystem model (MobilityDuck {@code scripts/parity-audit.py}
 * + {@code docs/DuckDB-Parity-Gaps.md}): every <em>addressable</em> MobilityDB
 * SQL function — {@code CREATE FUNCTION} minus PG-only index/gist/spgist
 * sections and {@code _in}/{@code _out}/{@code _recv}/{@code _send}/{@code _sel}
 * /{@code _transfn}/... plumbing suffixes — must be either:
 * <ol>
 *   <li>registered as a same-name Spark UDF, or</li>
 *   <li>listed in {@code spark-parity-exclusions.txt} with a category and
 *       reason ({@code RESHAPED} = covered under a bare/operator-derived name,
 *       {@code FAMILY_OOS} = family not built into this surface, {@code GAP} =
 *       tracked open gap).</li>
 * </ol>
 *
 * <p>A function that is neither covered nor accounted for fails the test. This
 * replaces the inflatable name-heuristic audit (which reported "99.3%" while
 * hiding signature gaps such as the {@code tgeompoint(tpose)} cast): the
 * coverage figure printed here is exact, and a re-pin that adds a new SQL
 * function or drops a UDF breaks the build until it is triaged.
 *
 * <p>Contract resources live in {@code src/test/resources/meos/} and are
 * regenerated on each re-pin from the pinned consolidated MobilityDB tip.
 */
public class SparkUdfParityTest {

  private static Set<String> readSet(String resource) {
    Set<String> out = new LinkedHashSet<>();
    try (InputStream in = SparkUdfParityTest.class.getResourceAsStream(resource)) {
      if (in == null) fail("Missing contract resource: " + resource);
      try (BufferedReader r = new BufferedReader(
             new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = r.readLine()) != null) {
          line = line.trim();
          if (line.isEmpty() || line.startsWith("#")) continue;
          out.add(line);
        }
      }
    } catch (Exception e) {
      fail("Could not read " + resource + ": " + e);
    }
    return out;
  }

  /** name -> category from the exclusions registry. */
  private static Map<String, String> readExclusions(String resource) {
    Map<String, String> out = new LinkedHashMap<>();
    for (String line : readSet(resource)) {
      String[] p = line.split("\t");
      if (p.length >= 2) out.put(p[0].toLowerCase(), p[1].trim());
    }
    return out;
  }

  private static Set<String> lower(Set<String> s) {
    Set<String> o = new TreeSet<>();
    for (String x : s) o.add(x.toLowerCase());
    return o;
  }

  @Test
  public void everyAddressableSqlFunctionIsCoveredOrAccounted() {
    Set<String> addressable = lower(readSet("/meos/mdb-sql-userfacing.txt"));
    Set<String> udfs = lower(readSet("/meos/spark-registered-udfs.txt"));
    Map<String, String> excl = readExclusions("/meos/spark-parity-exclusions.txt");

    assertTrue(addressable.size() > 500,
      "addressable surface looks stale: " + addressable.size());

    Set<String> covered = new TreeSet<>();
    Map<String, Integer> byCategory = new TreeMap<>();
    Set<String> unaccounted = new TreeSet<>();

    for (String fn : addressable) {
      if (udfs.contains(fn)) {
        covered.add(fn);
      } else if (excl.containsKey(fn)) {
        byCategory.merge(excl.get(fn), 1, Integer::sum);
      } else {
        unaccounted.add(fn);
      }
    }

    int reshaped = byCategory.getOrDefault("RESHAPED", 0);
    int oos = byCategory.getOrDefault("FAMILY_OOS", 0);
    int gap = byCategory.getOrDefault("GAP", 0);
    int inScope = addressable.size() - oos;

    System.out.println("=== Spark UDF parity (MobilityDB SQL surface, pinned tip) ===");
    System.out.printf("  Addressable user-facing SQL functions : %d%n", addressable.size());
    System.out.printf("  Exact same-name UDF                   : %d%n", covered.size());
    System.out.printf("  Reshaped (bare/operator-named UDF)     : %d%n", reshaped);
    System.out.printf("  Family out-of-scope                    : %d%n", oos);
    System.out.printf("  Tracked open GAPs                      : %d%n", gap);
    System.out.printf("  In-scope coverage (exact+reshaped)/(addr-oos) = %d/%d = %.1f%%%n",
      covered.size() + reshaped, inScope,
      100.0 * (covered.size() + reshaped) / inScope);

    if (!unaccounted.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      int n = 0;
      for (String s : unaccounted) {
        sb.append("\n    ").append(s);
        if (++n == 50) { sb.append("\n    ... (").append(unaccounted.size() - n)
          .append(" more)"); break; }
      }
      fail(unaccounted.size() + " addressable MobilityDB SQL functions are neither "
        + "registered as a Spark UDF nor accounted for in spark-parity-exclusions.txt "
        + "(triage each as RESHAPED / FAMILY_OOS / GAP, or implement the UDF):" + sb);
    }
  }
}
