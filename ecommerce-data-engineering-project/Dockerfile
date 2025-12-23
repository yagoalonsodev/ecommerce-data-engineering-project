# Dockerfile para Proyecto ETL Ecommerce Dataset
# Imagen base con Python 3.11
FROM python:3.11-slim

# Metadatos
LABEL maintainer="Proyecto ETL Ecommerce"
LABEL description="Container para procesos ETL con Pandas y PySpark - Modelo Dimensional"
LABEL version="1.0"

# Configurar el directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema necesarias para PySpark
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Configurar variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
ENV PATH=$PATH:$JAVA_HOME/bin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Configuración crítica para Java 17+ con Spark
# Estos flags permiten que Spark funcione correctamente con Java 17
ENV SPARK_SUBMIT_OPTS="--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
-Djava.security.manager=allow"

# Copiar archivo de requirements
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todos los archivos del proyecto
COPY . .

# Crear directorios necesarios
RUN mkdir -p /app/data /app/warehouse /app/notebooks /app/docs

# Exponer puerto para Jupyter Notebook (opcional)
EXPOSE 8888

# Comando por defecto: mostrar ayuda
CMD ["python", "-c", "print('\\n╔════════════════════════════════════════════════════════════╗'); print('║   PROYECTO ETL ECOMMERCE - Pandas & PySpark              ║'); print('║   Modelo Dimensional: fact_sales, dim_customer,        ║'); print('║   dim_product, dim_date                                ║'); print('╚════════════════════════════════════════════════════════════╝'); print('\\nComandos disponibles:\\n'); print('1. Limpieza de datos:'); print('   python limpieza_de_datos.py\\n'); print('2. ETL con Pandas:'); print('   python etl_warehouse.py\\n'); print('3. ETL con PySpark:'); print('   python etl_pyspark.py\\n'); print('4. Consultas del warehouse:'); print('   python warehouse/consultas_warehouse.py\\n'); print('5. Comparar warehouses:'); print('   python comparacion_warehouses.py\\n'); print('6. Resumen ejecutivo:'); print('   python resumen_ejecutivo.py\\n'); print('7. Jupyter Notebook:'); print('   jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root\\n')"]

