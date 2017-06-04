###############################################################################
# Script to install Apache Cassandra
###############################################################################

sudo add-apt-repository ppa:webupd8team/java
sudo apt update
sudo apt upgrade
sudo apt install oracle-java8-set-default
echo "deb http://www.apache.org/dist/cassandra/debian 310x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
echo "deb-src http://www.apache.org/dist/cassandra/debian 310x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 0353B12C
gpg --export --armor 0353B12C | sudo apt-key add -
sudo apt update
sudo apt install cassandra
