
aws s3 cp ./get_id.sh s3://r2d2-spark
aws s3 cp ./ip_mapping s3://r2d2-spark
aws s3 cp ./spark.toml s3://r2d2-spark
aws s3 cp ./run.sh s3://r2d2-spark
flintrock run-command bigd --master-only -- touch /home/ec2-user/am_master
flintrock run-command bigd -- aws s3 cp s3://r2d2-spark/ip_mapping /home/ec2-user
flintrock run-command bigd -- aws s3 cp s3://r2d2-spark/get_id.sh /home/ec2-user
flintrock run-command bigd -- aws s3 cp s3://r2d2-spark/r2d2_bin /home/ec2-user
flintrock run-command bigd -- aws s3 cp s3://r2d2-spark/spark.toml /home/ec2-user
flintrock run-command bigd -- aws s3 cp s3://r2d2-spark/run.sh /home/ec2-user
flintrock run-command bigd -- chmod +x /home/ec2-user/get_id.sh
flintrock run-command bigd -- chmod +x /home/ec2-user/r2d2_bin
flintrock run-command bigd -- chmod +x /home/ec2-user/run.sh



