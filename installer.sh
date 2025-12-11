echo "Installing Java + Python3 + pip..."
sudo apt-get update -y
sudo apt-get install -y openjdk-11-jdk python3 python3-pip curl

echo "Creating /jars directory for Sedona .jar files..."
sudo mkdir -p /jars

echo "Downloading sedona-spark-shaded jar (1.6.1) in /jars directory"
sudo curl -L -o /jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar \
    "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.6.1/sedona-spark-shaded-3.5_2.12-1.6.1.jar"

#Used by Sedona under the hood
echo "Downloading geotools-wrapper jar " 
sudo curl -L -o /jars/geotools-wrapper-1.6.1-28.2.jar \
    "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.6.1-28.2/geotools-wrapper-1.6.1-28.2.jar"

echo "Installing required Python libraries via pip..."
sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install apache-sedona[spark]==1.6.1

echo "Verifying Sedona jar files in /jars:"
ls /jars | grep sedona || echo "sedona jar NOT FOUND"
ls /jars | grep geotools || echo "geotools jar NOT FOUND"

echo "Setup complete! You can now run your Spark-Sedona jobs."
