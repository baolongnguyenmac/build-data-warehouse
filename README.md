# Learn Spark using PySpark

- Đây là đống code dùng để tiếp thu thứ kiến thức kỳ quái mang tên Spark
- Spark được viết bằng Java, PySpark là giao diện Python của nó
- Spark dùng để handle các file data có kích thước lớn. Nó tính toán trên RAM nên mọi thứ khá nhanh

## Các công cụ sử dụng

- Spark
- PySpark
- Pandas (đụng tới chút xíu)
- pymssql
- SQL Server

## Cài Spark và PySpark (cho MacOS)

- Có thể làm theo hướng dẫn trong [đường link này](https://medium.com/swlh/pyspark-on-macos-installation-and-use-31f84ca61400)
- Máy tui sử dụng MacOS và 3 version Java (nên có 3 con path cho java)
- Configure path in file `~/.zshrc` cho terminal ZSH hoặc `~/.bashrc` cho bash thông thường 
    ```txt
    # >>> setup java_home >>>
    export JAVA_16_HOME=/usr/local/opt/java/libexec/openjdk.jdk/Contents/Home
    export JAVA_HOME=/usr/local/opt/java11/libexec/openjdk.jdk/Contents/Home
    export JAVA_8_HOME=$(/usr/libexec/java_home -v1.8)

    alias java8='export JAVA_HOME=$JAVA_8_HOME'
    alias java11='export JAVA_HOME=$JAVA_11_HOME'
    alias java16='export JAVA_HOME=$JAVA_16_HOME'

    # default: java8 for running pyspark without any error
    java8

    # <<< setup java_home <<<

    export SPARK_HOME=/usr/local/Cellar/apache-spark/3.1.2/libexec
    export PATH=/usr/local/Cellar/apache-spark/3.1.2/bin:$PATH
    #export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"
    #export PYSPARK_PYTHON=jupyter
    #export PYSPARK_DRIVER_PYTHON='notebook'
    ```

## Cài PyMssql (cho MacOS)

- Có thể tham khảo tại [đường link này](https://github.com/pymssql/pymssql/issues/543)
- Cần phải cài `freetds` trước khi cài `pymssql`
    ```bash
    brew install 
    ```

- Sau đó cài Pymssql được kéo trực tiếp từ github về
    ```
    pip3 install git+https://github.com/pymssql/pymssql.git 
    ```

- Cầu nguyện cho mọi thứ work với nhau

## 2 folders bên trên

- `./Learn PySpark`: Có thể đọc về các sử dụng cơ bản của PySpark ở đây
- `./ETL`: Demo quá trình ETL dữ liệu từ source về data warehouse sẽ trông như thế nào. Có thể đọc cuốn `Buiding A Data Warehouse with Example (Vincent Rainardi)` để hiểu các khái niệm cơ bản, về quy trình ETL dữ liệu hay build 1 data warehouse sẽ diễn ra như thế nào
    - `./ETL/Raw Data`: Chứa 1 file data với gần 10.000 dòng dữ liệu giao dịch (Chính là file SuperStore trong dữ liệu demo của Tableau). Data từ 1 file chính (`customer_order.csv`) được cắt ra thành 5 file bằng nhau, mỗi file chứa khoảng 1998 dòng
    - `./ETL/SQL`: Chứa các schema databases của Source data, meta database, data warehouse
    - `./ETL/src`: Chứa source code mô phỏng quá trình source system thay đổi theo thời gian, và code mô phỏng quá trình handle các update, new creation trong source
