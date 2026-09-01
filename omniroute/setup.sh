sudo apt-get update

sudo apt-get install -y \
  git \
  curl \
  wget \
  unzip \
  netcat-openbsd \
  ca-certificates \
  gnupg \
  lsb-release \
  jq

git --version
nc -h | head
curl --version

sudo apt-get install -y docker.io

sudo systemctl enable --now docker

sudo usermod -aG docker $USER

docker --version
docker ps
