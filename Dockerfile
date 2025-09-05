# Usa una imagen base de R. Puedes elegir una versión específica.
FROM rocker/r-base:4.4.1

# Instala dependencias del sistema operativo que tu script R pueda necesitar
# Basado en tu YAML, parece que necesitas algunas librerías para curl, ssl, xml, v8
RUN apt-get update -y && \
    apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libv8-dev \
    && rm -rf /var/lib/apt/lists/*

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app
RUN mkdir -p /app/inst/secrets/
RUN mkdir -p /app/inst/styles/
RUN mkdir -p /app/scripts/

# Si tienes un archivo renv.lock, cópialo también
COPY renv.lock /app/renv.lock

COPY inst/secrets/vendu-tech-general-encrypted.json /app/inst/secrets/vendu-tech-general-encrypted.json
COPY inst/styles/logo_vendunegro-01.png /app/inst/styles/logo_vendunegro-01.png


# Instala renv y restaura las dependencias de R si usas renv
# Si no usas renv, puedes listar tus paquetes R directamente aquí con install.packages()
RUN R -e "install.packages('remotes')"
RUN R -e "remotes::install_version('renv', version = '1.0.7')"
RUN R -e "Sys.setenv(RENV_DOWNLOAD_METHOD = 'libcurl')"
# Si tienes un renv.lock, descomenta la siguiente línea y asegúrate de haber copiado renv.lock
RUN R -e "renv::restore(repos = getOption('repos'))"
# Copia tu script R y cualquier otro archivo necesario al contenedor
# Asegúrate de que la ruta sea correcta, por ejemplo, si tu script está en la carpeta 'scripts'# Set the entrypoint to a shell script
# ENTRYPOINT ["/bin/bash", "-c"]
# COPY scripts/ /app/scripts/
# COPY scripts/ /app/
COPY scripts/not_sales_alert.R /app/not_sales_alert.R
COPY scripts/helpers.R /app/scripts/helpers.R

# Comando que se ejecutará cuando el contenedor inicie
# Este comando ejecutará tu script R
# CMD ["Rscript", "$1"]
CMD ["Rscript", "not_sales_alert.R"]