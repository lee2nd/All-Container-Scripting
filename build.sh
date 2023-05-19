docker build --network=host --build-arg http_proxy=http://10.88.32.47:3128 --build-arg https_proxy=http://10.88.32.47:3128 -f Dockerfile -t python_dailymail . 
