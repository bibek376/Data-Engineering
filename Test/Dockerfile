FROM redhat/ubi8:8.7

RUN yum update -y && yum install -y nano 
RUN yum install -y mailx

WORKDIR /root

COPY .mailrc .mailrc
COPY mail.sh mail.sh
RUN chmod +x mail.sh

ENTRYPOINT [ "bash" ]

