#!/usr/bin/env bash
# Install Apache Spark 3.5.x and Maven for the MobilitySpark benchmark.
#
# What this script does:
#   1. Installs Maven via apt (for building the MobilitySpark fat JAR)
#   2. Downloads Apache Spark 3.5.4 to /opt/spark-3.5.4 and creates
#      a /opt/spark symlink
#   3. Adds SPARK_HOME and PATH entries to ~/.bashrc (or ~/.zshrc)
#
# After running this script, open a new shell (or source ~/.bashrc) and
# run the benchmark:
#   cd /path/to/MobilitySpark
#   mvn package -DskipTests -q
#   ./berlinmod/bench/bench.sh --skip-mbdb --skip-mduck

set -euo pipefail

SPARK_VERSION="3.5.4"
SPARK_HADOOP="hadoop3"
INSTALL_DIR="/opt"
SPARK_TARBALL="spark-${SPARK_VERSION}-bin-${SPARK_HADOOP}.tgz"
SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TARBALL}"

SHELL_RC="${HOME}/.bashrc"
[[ "${SHELL}" == */zsh ]] && SHELL_RC="${HOME}/.zshrc"

echo "╔══════════════════════════════════════════════════╗"
echo "║  MobilitySpark dependency installer              ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── Maven ─────────────────────────────────────────────────────────────────────
if command -v mvn >/dev/null 2>&1; then
  echo "[ok]  Maven already installed: $(mvn --version | head -1)"
else
  echo "=== Installing Maven ==="
  sudo apt-get update -q
  sudo apt-get install -y maven
  echo "[ok]  Maven installed: $(mvn --version | head -1)"
fi

# ── Apache Spark ──────────────────────────────────────────────────────────────
SPARK_HOME="${INSTALL_DIR}/spark-${SPARK_VERSION}-bin-${SPARK_HADOOP}"
SPARK_LINK="${INSTALL_DIR}/spark"

if [[ -x "${SPARK_LINK}/bin/spark-submit" ]]; then
  echo "[ok]  Spark already installed at ${SPARK_LINK}"
else
  echo ""
  echo "=== Downloading Apache Spark ${SPARK_VERSION} (~300 MB) ==="
  TMP=$(mktemp -d)
  trap 'rm -rf "$TMP"' EXIT

  curl -L --progress-bar "${SPARK_URL}" -o "${TMP}/${SPARK_TARBALL}"

  echo "=== Installing to ${SPARK_HOME} ==="
  sudo tar -xzf "${TMP}/${SPARK_TARBALL}" -C "${INSTALL_DIR}"
  sudo ln -sfn "${SPARK_HOME}" "${SPARK_LINK}"
  echo "[ok]  Spark installed: ${SPARK_HOME}"
fi

# ── PATH / SPARK_HOME in shell rc ─────────────────────────────────────────────
if grep -q "SPARK_HOME=${INSTALL_DIR}/spark" "${SHELL_RC}" 2>/dev/null; then
  echo "[ok]  SPARK_HOME already in ${SHELL_RC}"
else
  echo ""
  echo "=== Adding SPARK_HOME to ${SHELL_RC} ==="
  cat >> "${SHELL_RC}" << 'EOF'

# Apache Spark (added by MobilitySpark setup/install_spark.sh)
export SPARK_HOME=/opt/spark
export PATH="$PATH:$SPARK_HOME/bin"
EOF
  echo "[ok]  Added SPARK_HOME=/opt/spark and PATH update to ${SHELL_RC}"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  Installation complete                           ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
echo "Next steps:"
echo ""
echo "  1. Open a new shell (or run: source ${SHELL_RC})"
echo "  2. Build the MobilitySpark JAR:"
echo "       cd $(cd "$(dirname "$0")/.." && pwd)"
echo "       mvn package -DskipTests -q"
echo "  3. Run the benchmark:"
echo "       ./berlinmod/bench/bench.sh --skip-mbdb --skip-mduck"
echo "     Or all three platforms:"
echo "       ./berlinmod/bench/bench.sh"
echo ""
echo "spark-submit is at: ${SPARK_LINK}/bin/spark-submit"
