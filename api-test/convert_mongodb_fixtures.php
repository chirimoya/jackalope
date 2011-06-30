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

            $attrs = array();
            foreach ($node->childNodes AS $child) {
                if ($child instanceof DOMElement && $child->tagName == "sv:property") {
                    $name = $child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'name');

                    $value = array();
                    foreach ($child->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'value') AS $nodeValue) {
                        $value[] = $nodeValue->nodeValue;
                    }

                    $isMulti = (in_array($name, array('jcr:mixinTypes'))) || count($value) > 1;
                    $attrs[$name] = array(
                        'type' =>  strtolower($child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'type')),
                        'value' => ($isMulti) ? $value : current($value),
                        'multi' => $isMulti,
                    );
                }
            }

            if (isset($attrs['jcr:uuid']['value'])) {
                $id = (string) $attrs['jcr:uuid']['value'];
                $id = new \MongoBinData($id, MongoBinData::UUID);
                $id = array('$binary' => base64_encode($id->bin), '$type' => (string) sprintf('%02d', $id->type));
                unset($attrs['jcr:uuid']);
            }else{  
                $id = new \MongoId;
                $id = array('$oid' =>  $id->__toString());
            }
            
            $type = $attrs['jcr:primaryType']['value'];
            unset($attrs['jcr:primaryType']);
            
            $parentPath = implode("/", array_slice(explode("/", $path), 0, -1));
            $parentPath = ($parentPath == '') ? '/' : $parentPath;
            
            $dataSet[] = array(
                '_id' => $id,
                'path' => $path,
                'parent' => $parentPath,
                'w_id' => array('$oid' => '4e00e8fea381601b08000000'),
                'type' => $type,
                'props' => $attrs
            );
        }
    } else {
        
        $nodes = $srcDom->getElementsByTagName('*');
        foreach ($nodes AS $node) {
            if ($node instanceof DOMElement) {
                $parent = $node;
                $path = "";
                do {
                    $path = "/" . $parent->tagName . $path;
                    $parent = $parent->parentNode;
                } while ($parent instanceof DOMElement);

                $attrs = array();
                foreach ($node->attributes AS $attr) {
                    $name = ($attr->prefix) ? $attr->prefix.":".$attr->name : $attr->name;
                    $attrs[$name] = $attr->value;
                }

                if (!isset($attrs['jcr:primaryType'])) {
                    $attrs['jcr:primaryType'] = 'nt:unstructured';
                }

                if (isset($attrs['jcr:uuid'])) {
                    $id = (string) $attrs['jcr:uuid'];
                    $id = new \MongoBinData($id, MongoBinData::UUID);
                    $id = array('$binary' => $id->__toString(), '$type' => MongoBinData::UUID);
                    unset($attrs['jcr:uuid']);
                }else{  
                    $id = new \MongoId;
                    $id = array('$oid' =>  $id->__toString());
                }
                    
                $type = $attrs['jcr:primaryType'];
                unset($attrs['jcr:primaryType']);
                
                if (!isset($seenPaths[$path])) {
                    $parentPath = implode("/", array_slice(explode("/", $path), 0, -1));
                    $parentPath = ($parentPath == '') ? '/' : $parentPath;
            
                    $dataSet[] = array(
                        '_id' => $id,
                        'path' => $path,
                        'parent' => $parentPath,
                        'w_id' => array('$oid' => '4e00e8fea381601b08000000'),
                        'type' => 'nt:unstructured',
                        'props' => $attrs
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