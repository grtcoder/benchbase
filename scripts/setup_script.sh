sudo apt update
sudo apt install crony
sudo apt install git

# Setup and install go
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz

# Set up environment variables
echo "export PATH=\$PATH:/usr/local/go/bin" >> ~/.bashrc
source ~/.bashrc


git clone https://github.com/grtcoder/lock-free-machine.git
cd lock-free-machine