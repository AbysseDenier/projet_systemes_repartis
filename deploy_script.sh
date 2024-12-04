#!/bin/bash
login="eeskinazi-24"
localFolder="./"
todeploy="dossierAdeployer"
remoteFolder="bgd701eskinazi"
nameOfTheScript="script_worker.py"

# liste des machines
computers=($(cat machines.txt))

# Création du répertoire distant remoteFolder
command1=("ssh" "-tt" "$login@${computers[0]}" "rm -rf $remoteFolder; mkdir $remoteFolder;wait;")
echo ${command1[*]}
"${command1[@]}";wait;

# Copie des fichiers du dossier todeploy dans le répertoire distant
command2=("scp" "-r" "$localFolder$todeploy" "$login@${computers[0]}:$remoteFolder")
echo ${command2[*]}
"${command2[@]}";wait;

# Lance script python nameOfTheScript sur chaque machine distante
for c in ${computers[@]}; do
  command3=("ssh" "-tt" "$login@$c" "cd $remoteFolder/$todeploy; python3 $nameOfTheScript; wait;")
  echo ${command3[*]}
  "${command3[@]}" &
done


