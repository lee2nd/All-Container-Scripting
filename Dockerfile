FROM python:3.10.9
RUN addgroup --system --gid 888 auo \
    && useradd -r -u 1009 -g 888 wma

COPY pip.conf /etc/pip.conf

# install env and tools
RUN pip install --upgrade pip
RUN pip install jupyter
RUN pip install numpy pandas schedule openpyxl
RUN pip install zeep
RUN pip install xlsxwriter
RUN pip install pymongo 

# using cache
WORKDIR /Dailymail
COPY ./requirements.txt ./requirements.txt
RUN pip install --upgrade pip && pip install -r requirements.txt
RUN mkdir /home/wma
RUN chmod 777 /home/wma
USER wma
EXPOSE 8080

WORKDIR /Dailymail/data/code
CMD ["python", "dailyreport.py"]
