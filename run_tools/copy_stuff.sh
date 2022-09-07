aws s3 cp ./get_id.sh s3://r2d2-spark
aws s3 cp ./ip_mapping s3://r2d2-spark
flintrock run-command my-spark-cluster -- aws s3 cp s3://r2d2-spark/ip_mapping /home/ec2-user
flintrock run-command my-spark-cluster -- aws s3 cp s3://r2d2-spark/get_id.sh /home/ec2-user
flintrock run-command my-spark-cluster -- aws s3 cp s3://r2d2-spark/r2d2_bin /home/ec2-user
flintrock run-command my-spark-cluster -- chmod +x /home/ec2-user/get_id.sh



