<?php
/**
 * Convert Jackalope Document or System Views into MongoDb Fixture files
 *
 * @author Thomas Schedler <thomas@chirimoya.at>
 */

require_once __DIR__ . "/../src/Jackalope/Helper.php";

$srcDir = __DIR__ . "/suite/fixtures";
$destDir = __DIR__ . "/fixtures/mongodb";

$jcrTypes = array(
    "string"        => array(1, "clob_data"),
    "binary"        => array(2, "int_data"),
    "long"          => array(3, "int_data"),
    "double"        => array(4, "float_data"),
    "date"          => array(5, "datetime_data"),
    "boolean"       => array(6, "int_data"),
    "name"          => array(7, "string_data"),
    "path"          => array(8, "string_data"),
    "reference"     => array(9, "string_data"),
    "weakreference" => array(10, "string_data"),
    "uri"           => array(11, "string_data"),
    "decimal"       => array(12, "string_data"),
);

$rdi = new RecursiveDirectoryIterator($srcDir);
$ri = new RecursiveIteratorIterator($rdi);

libxml_use_internal_errors(true);
foreach ($ri AS $file) {
    if (!$file->isFile()) { continue; }

    $newFile = str_replace('.xml', '.json', str_replace($srcDir, $destDir, $file->getPathname()));

    $srcDom = new DOMDocument('1.0', 'UTF-8');
    $srcDom->load($file->getPathname());

    if (libxml_get_errors()) {
        echo "Errors in " . $file->getPathname()."\n";
        continue;
    }

    echo "Importing " . str_replace($srcDir, "", $file->getPathname())."\n";
    $dataSet = array();

    $nodes = $srcDom->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'node');
    $seenPaths = array();
    if ($nodes->length > 0) {
        $id = new \MongoId();
        // system-view
        $dataSet[] = array(
            '_id' => array('$oid' => $id->__toString()),
            'uuid' => '',
            'path' => '',
            'parent' => '-1',
            'workspace_id' => array('$ref' => 'jcrworkspaces', '$id' => '4e00e8fea381601b08000000'),
            'type' => 'nt:unstructured',
            'properties' => array()
        );
        
        foreach ($nodes AS $node) {
            /* @var $node DOMElement */
            $parent = $node;
            $path = "";
            do {
                if ($parent->tagName == "sv:node") {
                    $path = "/" . $parent->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'name') . $path;
                }
                $parent = $parent->parentNode;
            } while ($parent instanceof DOMElement);
            $path = ltrim($path, '/');

            $attrs = array();
            foreach ($node->childNodes AS $child) {
                if ($child instanceof DOMElement && $child->tagName == "sv:property") {
                    $name = $child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'name');

                    $value = array();
                    foreach ($child->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'value') AS $nodeValue) {
                        $value[] = $nodeValue->nodeValue;
                    }

                    $attrs[$name] = array(
                        'type' =>  strtolower($child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'type')),
                        'value' => $value,
                        'multiValued' => (in_array($name, array('jcr:mixinTypes'))) || count($value) > 1,
                    );
                }
            }

            $uuid = '';
            if (isset($attrs['jcr:uuid']['value'][0])) {
                $id = (string) $attrs['jcr:uuid']['value'][0];
                unset($attrs['jcr:uuid']['value'][0]);
            }
            
            $id = new \MongoId;
            $id = $id->__toString();
            
            $type = $attrs['jcr:primaryType']['value'][0];
            unset($attrs['jcr:primaryType']);
            
            $dataSet[] = array(
                '_id' => array('$oid' => $id),
                'uuid' => $uuid,
                'path' => $path,
                'parent' => implode("/", array_slice(explode("/", $path), 0, -1)),
                'workspace_id' => array('$ref' => 'jcrworkspaces', '$id' => '4e00e8fea381601b08000000'),
                'type' => $type,
                'properties' => $attrs
            );
        }
    } else {
        $id = new \MongoId;
        // document-view
        $dataSet[] = array(
            '_id' => array('$oid' => $id->__toString()),
            'path' => '',
            'parent' => '-1',
            'workspace_id' => array('$ref' => 'jcrworkspaces', '$id' => '4e00e8fea381601b08000000'),
            'type' => 'nt:unstructured',
            'properties' => array()
        );

        $nodes = $srcDom->getElementsByTagName('*');
        foreach ($nodes AS $node) {
            if ($node instanceof DOMElement) {
                $parent = $node;
                $path = "";
                do {
                    $path = "/" . $parent->tagName . $path;
                    $parent = $parent->parentNode;
                } while ($parent instanceof DOMElement);
                $path = ltrim($path, '/');

                $attrs = array();
                foreach ($node->attributes AS $attr) {
                    $name = ($attr->prefix) ? $attr->prefix.":".$attr->name : $attr->name;
                    $attrs[$name] = $attr->value;
                }

                if (!isset($attrs['jcr:primaryType'])) {
                    $attrs['jcr:primaryType'] = 'nt:unstructured';
                }

                $uuid = '';
                if (isset($attrs['jcr:uuid'])) {
                    $uuid = $attrs['jcr:uuid'];
                    unset($attrs['jcr:uuid']);
                }
                
                $id = new \MongoId();
                $id = $id->__toString();
                    
                $type = $attrs['jcr:primaryType'];
                unset($attrs['jcr:primaryType']);
                
                if (!isset($seenPaths[$path])) {
                    $dataSet[] = array(
                        '_id' => array('$oid' => $id),
                        'uuid' => $uuid,
                        'path' => $path,
                        'parent' => implode("/", array_slice(explode("/", $path), 0, -1)),
                        'workspace_id' => array('$ref' => 'jcrworkspaces', '$id' => '4e00e8fea381601b08000000'),
                        'type' => 'nt:unstructured',
                        'properties' => $attrs
                    );
                    $seenPaths[$path] = $id;
                } else {
                    $id = $seenPaths[$path];
                }
            }
        }
    }

    @mkdir (dirname($newFile), 0777, true);
    file_put_contents($newFile, json_encode($dataSet));
}