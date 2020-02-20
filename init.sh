mkdir /usr/local/pbx
mkdir /usr/local/pbx/sbin
mkdir /usr/local/pbx/etc
mkdir /usr/local/pbx/config_all
mkdir /usr/local/pbx/incidents
mkdir /usr/local/pbx/listerm
mkdir /usr/local/pbx/listout
mkdir /usr/local/pbx/maodat
mkdir /usr/local/pbx/maohist
mkdir /usr/local/pbx/multiline
mkdir /usr/local/pbx/taxa
useradd isdn -d /usr/local/pbx
usermod -a -G postgres isdn
chown -R isdn:isdn /usr/local/pbx
chmod -R 700 /usr/local/pbx
chmod -R 770 /usr/local/pbx/config_all
chmod -R 770 /usr/local/pbx/incidents
chmod -R 770 /usr/local/pbx/listerm
chmod -R 770 /usr/local/pbx/maodat
chmod -R 770 /usr/local/pbx/maohist
chmod -R 770 /usr/local/pbx/multiline
chmod -R 770 /usr/local/pbx/taxa

umask
setfacl    -R --set u::rwx,g::rw-,o::--- /usr/local/pbx/incidents
setfacl -d -R --set u::rwx,g::rw-,o::--- /usr/local/pbx/incidents

 
	
   

