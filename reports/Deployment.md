Open a GCP machine with the bitnami image for rabbitmq
https://console.cloud.google.com/marketplace/details/bitnami-launchpad/rabbitmq?project=forward-subject-254912

After that run the shell script in `code/setup.sh`
This should setup all the packages. Now just run all the server scripts on nohup

To setup the FTP server:

`sudo /usr/sbin/useradd ftpserver`

Type the password also as `ftpserver`

Then update the line in `etc/init.d/sshd.config` from

`passwordAuthentication no`
 to
`passwordAuthentication yes`

then run

`sudo systemctl restart sshd`

and this should setup the FTP server for user 'ftpserver'
