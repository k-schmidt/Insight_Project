###############################################################################
# Run Flask API
###############################################################################

export RDS_HOST="insight-prod-cluster.cluster-ca1epl6w2hcd.us-east-1.rds.amazonaws.com"
export RDS_USERNAME="root"
export RDS_PASSWORD="\$chm!d2k"
GITBRANCH=development
python run.py
