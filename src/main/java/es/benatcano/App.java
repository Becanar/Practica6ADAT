package es.benatcano;

import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceIterator;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.modules.CollectionManagementService;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.XQueryService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class App {
    private static final String BASE_URI = "xmldb:exist://localhost:8080/exist/xmlrpc/db";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) throws Exception {

        Class<?> dbClass = Class.forName("org.exist.xmldb.DatabaseImpl");
        Database database = (Database) dbClass.getDeclaredConstructor().newInstance();
        DatabaseManager.registerDatabase(database);
        System.out.println("Conexión establecida con la base de datos: " + BASE_URI);

        Collection rootCollection = DatabaseManager.getCollection(BASE_URI, DB_USER, DB_PASSWORD);
        if (rootCollection == null) {
            System.err.println("Error: No se pudo conectar a la colección raíz.");
            return;
        }

        Collection gimnasioCollection = obtenerOCrearColeccion(rootCollection, "GIMNASIO");

        cargarDocumentoXML(gimnasioCollection, "src/main/resources/xml/socios_gim.xml");
        cargarDocumentoXML(gimnasioCollection, "src/main/resources/xml/actividades_gim.xml");
        cargarDocumentoXML(gimnasioCollection, "src/main/resources/xml/uso_gimnasio.xml");

        String xqueryIntermedio = construirXQueryIntermedio();
        ejecutarConsultaYGuardarResultado(gimnasioCollection, xqueryIntermedio, "cuotas_adicionales.xml");

        String xqueryFinal = construirXQueryCuotaFinal();
        ejecutarConsultaYGuardarResultado(gimnasioCollection, xqueryFinal, "cuotas_finales.xml");
    }

    private static Collection obtenerOCrearColeccion(Collection parentCollection, String collectionName) throws Exception {
        Collection collection = parentCollection.getChildCollection(collectionName);
        if (collection == null) {
            CollectionManagementService managementService = (CollectionManagementService) parentCollection.getService("CollectionManagementService", "1.0");
            collection = managementService.createCollection(collectionName);
            System.out.println("Colección creada: " + collectionName);
        } else {
            System.out.println("La colección ya existe: " + collectionName);
        }
        return collection;
    }

    private static void cargarDocumentoXML(Collection collection, String filePath) throws Exception {
        File archivo = new File(filePath);
        if (!archivo.canRead()) {
            System.err.println("Error: No se puede leer el archivo: " + filePath);
        } else {
            Resource resource = collection.createResource(archivo.getName(), "XMLResource");
            resource.setContent(archivo);
            collection.storeResource(resource);
            System.out.println("Documento cargado: " + filePath);
        }
    }

    private static String construirXQueryIntermedio() {
        return """
            let $socios := doc('socios_gim.xml')/SOCIOS_GIM/fila_socios
            let $actividades := doc('actividades_gim.xml')/ACTIVIDADES_GIM/fila_actividades
            let $uso := doc('uso_gimnasio.xml')/USO_GIMNASIO/fila_uso
            for $u in $uso
            let $socio := $socios[COD = $u/CODSOCIO][1]
            let $actividad := $actividades[@cod = $u/CODACTIV][1]
            let $horas := xs:integer($u/HORAFINAL) - xs:integer($u/HORAINICIO)
            let $cuota_adicional := 
                if ($actividad/@tipo = '1') then 0
                else if ($actividad/@tipo = '2') then $horas * 2
                else if ($actividad/@tipo = '3') then $horas * 4
                else 0
            return 
                <datos>
                    <COD>{data($u/CODSOCIO)}</COD>
                    <NOMBRESOCIO>{data($socio/NOMBRE)}</NOMBRESOCIO>
                    <CODACTIV>{data($u/CODACTIV)}</CODACTIV>
                    <NOMBREACTIVIDAD>{data($actividad/NOMBRE)}</NOMBREACTIVIDAD>
                    <horas>{$horas}</horas>
                    <tipoact>{data($actividad/@tipo)}</tipoact>
                    <cuota_adicional>{$cuota_adicional}</cuota_adicional>
                </datos>
        """;
    }

    private static String construirXQueryCuotaFinal() {
        return """
            let $socios := doc('socios_gim.xml')/SOCIOS_GIM/fila_socios
            let $cuotas := doc('cuotas_adicionales.xml')/datos
            for $socio in $socios
            let $suma_cuota_adic := sum($cuotas[COD = $socio/COD]/cuota_adicional)
            let $cuota_total := $suma_cuota_adic + xs:decimal($socio/CUOTA_FIJA)
            return 
                <datos>
                    <COD>{data($socio/COD)}</COD>
                    <NOMBRESOCIO>{data($socio/NOMBRE)}</NOMBRESOCIO>
                    <CUOTA_FIJA>{data($socio/CUOTA_FIJA)}</CUOTA_FIJA>
                    <suma_cuota_adic>{$suma_cuota_adic}</suma_cuota_adic>
                    <cuota_total>{$cuota_total}</cuota_total>
                </datos>
        """;
    }

    private static void ejecutarConsultaYGuardarResultado(Collection collection, String xquery, String fileName) throws Exception {
        XQueryService queryService = (XQueryService) collection.getService("XQueryService", "1.0");
        ResourceSet resultSet = queryService.query(xquery);
        XMLResource resource = (XMLResource) collection.createResource(fileName, "XMLResource");
        StringBuilder resultContent = new StringBuilder("<result>");

        ResourceIterator iterator = resultSet.getIterator();
        while (iterator.hasMoreResources()) {
            Resource resourceItem = iterator.nextResource();
            resultContent.append(resourceItem.getContent());
        }

        resultContent.append("</result>");
        resource.setContent(resultContent.toString());
        collection.storeResource(resource);
        System.out.println("Documento generado y guardado en la base de datos: " + fileName);

        Path outputDir = Paths.get("src/main/resources/xml_creados");
        if (!Files.exists(outputDir)) {
            Files.createDirectories(outputDir);  // Crear el directorio si no existe
        }

        Path outputFile = outputDir.resolve(fileName);
        Files.write(outputFile, resultContent.toString().getBytes());
        System.out.println("Documento guardado en recursos locales: " + outputFile);
    }
}
