#FROM bde2020/hbase-base:1.0.0-hbase1.2.6
FROM hbase-base:latest

COPY run.sh /run.sh
RUN chmod +x /run.sh

EXPOSE 16020 16030

CMD ["/run.sh"]
