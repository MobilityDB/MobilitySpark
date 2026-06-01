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

import functions.GeneratedFunctions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Explicit proof that the MEOS facade ({@code functions.GeneratedFunctions})
 * binds the full public MEOS C API exported by the pinned libmeos.
 *
 * <p>The API contract lives in {@code src/test/resources/meos/}, derived at
 * each re-pin from the consolidated MobilityDB tip:
 * <ul>
 *   <li>{@code meos-exported-symbols.txt} — every function symbol exported by
 *       {@code libmeos.so} (nm -D) that is also declared in a public header.
 *       Every one of these MUST have a binding.</li>
 *   <li>{@code meos-public-api.txt} — every function declared in a public
 *       header (a superset that also includes symbols not yet exported).</li>
 *   <li>{@code meos-declared-not-exported.txt} — declared but not yet exported
 *       by libmeos (pending kernel impl / build-flag-gated). Bindings are
 *       generated but would not link at runtime; tracked, not silently
 *       dropped.</li>
 *   <li>{@code meos-internal-exported-helpers.txt} — exported but intentionally
 *       undeclared internal helpers ({@code *_restrict_geom},
 *       {@code *_restrict_stbox}, {@code *_distance_turnpt}); NOT part of the
 *       contract — the public wrappers are bound instead.</li>
 * </ul>
 *
 * <p>This test replaces unverified "100% parity" claims with a check that
 * fails CI the moment a public symbol loses its binding or a re-pin adds a
 * symbol the facade does not cover.
 */
public class MeosApiCoverageTest {

  /** Public static method names declared on the generated facade. */
  private static Set<String> facadeBindings() {
    Set<String> names = new TreeSet<>();
    for (Method m : GeneratedFunctions.class.getDeclaredMethods()) {
      int mod = m.getModifiers();
      if (Modifier.isPublic(mod) && Modifier.isStatic(mod)) {
        names.add(m.getName());
      }
    }
    return names;
  }

  private static Set<String> readContract(String resource) {
    Set<String> names = new LinkedHashSet<>();
    try (InputStream in =
           MeosApiCoverageTest.class.getResourceAsStream(resource)) {
      if (in == null) {
        fail("Missing API-contract resource: " + resource);
      }
      try (BufferedReader r = new BufferedReader(
             new InputStreamReader(in, StandardCharsets.UTF_8))) {
        String line;
        while ((line = r.readLine()) != null) {
          line = line.trim();
          if (line.isEmpty() || line.startsWith("#")) continue;
          names.add(line);
        }
      }
    } catch (Exception e) {
      fail("Could not read " + resource + ": " + e);
    }
    return names;
  }

  private static String sample(Set<String> missing) {
    StringBuilder sb = new StringBuilder();
    int n = 0;
    for (String s : missing) {
      sb.append("\n    ").append(s);
      if (++n == 40) { sb.append("\n    ... (").append(missing.size() - n)
        .append(" more)"); break; }
    }
    return sb.toString();
  }

  /**
   * Every public MEOS symbol exported by the pinned libmeos has a facade
   * binding. This is the load-bearing coverage proof: a missing binding here
   * means a runtime {@code UnsatisfiedLinkError} is reachable for a public
   * function the API promises.
   */
  @Test
  public void everyExportedMeosSymbolHasBinding() {
    Set<String> exported = readContract("/meos/meos-exported-symbols.txt");
    Set<String> bound = facadeBindings();
    assertTrue(exported.size() > 2500,
      "API contract looks empty/stale: " + exported.size() + " symbols");

    Set<String> missing = new TreeSet<>(exported);
    missing.removeAll(bound);
    if (!missing.isEmpty()) {
      fail(missing.size() + " of " + exported.size()
        + " exported MEOS symbols have no GeneratedFunctions binding:"
        + sample(missing));
    }
  }

  /**
   * Every function declared in a public header has a facade binding — including
   * symbols not yet exported by libmeos (their bindings must still be generated
   * so the codegen never silently drops part of the declared surface).
   */
  @Test
  public void everyPublicApiSymbolHasBinding() {
    Set<String> api = readContract("/meos/meos-public-api.txt");
    Set<String> bound = facadeBindings();

    Set<String> missing = new TreeSet<>(api);
    missing.removeAll(bound);
    if (!missing.isEmpty()) {
      fail(missing.size() + " of " + api.size()
        + " public-API symbols are absent from GeneratedFunctions (codegen drop):"
        + sample(missing));
    }
  }
}
