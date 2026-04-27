#!/bin/bash

# Datei ausführbar machen mit chmod +x setup_docker.sh

# Beende das Skript sofort, wenn ein kritischer Fehler auftritt
set -e

echo "🚀 Starte Setup für .NET Aspire via Docker Compose auf dem Raspberry Pi..."
echo "------------------------------------------------------------------"

# 1. System aktualisieren
echo "📦 1. Aktualisiere Paketquellen..."
sudo apt-get update && sudo apt-get upgrade -y

# 2. Docker installieren (falls nicht vorhanden)
if ! command -v docker &> /dev/null; then
    echo "🐳 2. Docker ist nicht installiert. Installiere Docker..."
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    rm get-docker.sh
    echo "✅ Docker wurde erfolgreich installiert. (Version: $(docker --version))"
else
    echo "✅ 2. Docker ist bereits installiert. (Version: $(docker --version))"
fi

# 3. Berechtigungen setzen
echo "🔑 3. Füge aktuellen User ($USER) zur Docker-Gruppe hinzu..."
sudo usermod -aG docker $USER

echo "✅ 3. Benutzerberechtigungen für Docker gesetzt. (Du musst dich ab- und wieder anmelden, damit die Änderungen wirksam werden.)"