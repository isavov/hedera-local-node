#!/bin/zsh
echo -e "\e[1;32m>Stop old instances (just in case)\e[0m"
node cli.js stop
echo -e "\e[1;32m>Cleanup old data\e[0m"
rm -rf compose-network/network-node/data/saved
echo -e "\e[1;32m>Start streams uploader and importer\e[0m"
docker-compose up -d importer record-streams-uploader
echo -e "\e[1;32m>Create symlink for apps dir\e[0m"
ln -sf ../../../../hedera-services/hedera-node/data/apps compose-network/network-node/data
echo -e "\e[1;31mYou should now run the services node in your IDE.\e[0m"
echo -e "\e[1;31mThen, press any key to continue...\e[0m"; read -k1 -s
echo -e "\e[1;32m>Running other services\e[0m"
docker-compose up -d
echo -e "\e[1;32m>Generating accounts\e[0m"
node cli.js generate-accounts init