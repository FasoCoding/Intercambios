# Intercambios SEN-SADI
Aplicaicón para la determinación de la capacidad de exportación de energía entre SEN y SADI.

## Funcionamiento.
A partir de la barra Andes 220, se recorre la subzona de curtailment y se agrega el total de curtailment por hora. El valor se limita por hora al máximo exportable (80 MW).

## Uso de la APP.
Instalar mediante pipx y pasar como argumento la ruta del accdb con la salida de plexos de la carpeta Datos.

## TODO
agregar pruebas unitarias y mejorar documentación.
Código actual está testeado sobre salidas reales de PRG.
