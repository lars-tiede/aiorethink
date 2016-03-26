# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  # port for RethinkDB's web ui
  config.vm.network "forwarded_port", guest: 8080, host: 8080,
      host_ip: "127.0.0.1",
      auto_correct: true

  # port for RethinkDB client connections
  config.vm.network "forwarded_port", guest: 28015, host: 28015,
      host_ip: "127.0.0.1",
      auto_correct: true

  config.vm.provision "shell", inline: <<-SHELL
    # Python 3.5
    sudo add-apt-repository -y ppa:fkrull/deadsnakes
    sudo apt-get update
    sudo apt-get install -y python3.5-complete

    # some other basics
    sudo apt-get install -y git

    # pip
    curl -O https://bootstrap.pypa.io/get-pip.py
    sudo python3.5 get-pip.py

    # tox
    sudo pip install tox

    # RethinkDB server (see http://rethinkdb.com/docs/install/ubuntu/)
    source /etc/lsb-release && echo "deb http://download.rethinkdb.com/apt $DISTRIB_CODENAME main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list
    wget -qO- https://download.rethinkdb.com/apt/pubkey.gpg | sudo apt-key add -
    sudo apt-get update
    sudo apt-get install -y rethinkdb
    sudo cp /etc/rethinkdb/default.conf.sample /etc/rethinkdb/instances.d/instance1.conf
    echo "bind=all" | sudo tee -a /etc/rethinkdb/instances.d/instance1.conf
    sudo /etc/init.d/rethinkdb restart
  SHELL
end

# TODO: rig up ipython / jupyter with the await extension. Instant aiorethink REPL!
