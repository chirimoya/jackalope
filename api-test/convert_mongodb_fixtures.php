<?php
/**
 * Convert Jackalope Document or System Views into MongoDb Fixture files
 *
 * @author Thomas Schedler <thomas@chirimoya.at>
 */

require_once __DIR__ . "/../lib/phpcr/src/PHPCR/Util/UUIDHelper.php";

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
    $pos = 0;

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

            $uuid = \PHPCR\Util\UUIDHelper::generateUUID();
            $attrs = array();
            foreach ($node->childNodes AS $child) {
                if ($child instanceof DOMElement && $child->tagName == "sv:property") {
                    $name = $child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'name');

                    $value = array();
                    foreach ($child->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'value') AS $nodeValue) {
                        $value[] = $nodeValue->nodeValue;
                    }

                    if($name == 'jcr:uuid') {
                        $uuid = current($value);
                    } else if($name == 'jcr:primaryType') {
                        $type = current($value); 
                    } else {
                        $isMulti = (in_array($name, array('jcr:mixinTypes'))) || count($value) > 1;
                        $propertyType = $child->getAttributeNS('http://www.jcp.org/jcr/sv/1.0', 'type');
                        
                        switch ($propertyType) {
                            case 'Binary':
                                $binaries = array();
                                foreach($value as $binary){
                                    $binaries[] = strlen(base64_decode($binary));
                                    
                                    //TODO store binaries, but how ?? 
                                }
                                $value = $binaries;
                                break;
                            case 'Date':
                                $dates = array();
                                foreach($value as $date){
                                    $datetime = \DateTime::createFromFormat('Y-m-d\TH:i:s.uP', $date);
                                    $datetime->modify('-1 hour');
                                    $datetime->setTimezone(new DateTimeZone('Europe/London'));
                                    
                                    $dates[] = array(
                                        'date' => array('$date' => $datetime->getTimestamp() * 1000), 
                                        'timezone' => $datetime->getTimezone()->getName()
                                    );
                                    
                                }
                                $value = $dates;
                                break;
                        }
                        
                        $attrs[] = array(
                            'name' => $name,
                            'type' => $propertyType,
                            'value' => ($isMulti) ? $value : current($value),
                            'multi' => $isMulti,
                        );
                    }
                }
            }

            $id = new \MongoBinData($uuid, MongoBinData::UUID);
            $id = array('$binary' => base64_encode($id->bin), '$type' => (string) sprintf('%02d', $id->type));
          
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
            
            $pos++;
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

                $uuid = \PHPCR\Util\UUIDHelper::generateUUID();
                $type = 'nt:unstructured';
                $attrs = array();
                foreach ($node->attributes AS $attr) {
                    $name = ($attr->prefix) ? $attr->prefix.":".$attr->name : $attr->name;
                    
                    if($name == 'jcr:uuid') {
                        $uuid = $attr->value;
                    } else if($name == 'jcr:primaryType') {
                        $type = $attr->value; 
                    } else {
                        $attrs[] = array(
                            'name' => $name,
                            'value' => $attr->value,
                        );    
                    }
                }

                $id = new \MongoBinData($uuid, MongoBinData::UUID);
                $id = array('$binary' => $id->__toString(), '$type' => MongoBinData::UUID);
                
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
                    $seenPaths[$path] = $pos;
                    $pos++; 
                } else {
                    $idx = $seenPaths[$path];
                    $dataSet[$pos]['props'] = array_merge($dataSet[$pos]['props'], $attrs);
                }
            }
        }
    }

    @mkdir (dirname($newFile), 0777, true);
    file_put_contents($newFile, json_encode($dataSet));
}