###############################################################################
# Scipt to Install Kafka
###############################################################################

sudo apt update
sudo apt upgrade

sudo add-apt-repository ppa:webupd8team/java
sudo apt update
sudo apt-get install oracle-java8-installer

wget http://mirror.cc.columbia.edu/pub/software/apache/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
tar zxf zookeeper-3.4.6.tar.gz
sudo mv zookeeper-3.4.6 /usr/local/zookeeper
sudo mkdir -p /var/lib/zookeeper
export JAVA_HOME=/usr/lib/jvm/java-8-oracle/jre
