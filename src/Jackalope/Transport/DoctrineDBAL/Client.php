<?php
namespace Jackalope\Transport\DoctrineDBAL;

use PHPCR\PropertyType;
use PHPCR\RepositoryException;
use PHPCR\Util\UUIDHelper;
use Doctrine\DBAL\Connection;
use Doctrine\Common\Cache\Cache;
use Doctrine\Common\Cache\ArrayCache;
use Jackalope\TransportInterface;
use Jackalope\NodeType\NodeTypeManager;
use Jackalope\NodeType\PHPCR2StandardNodeTypes;

/**
 * Class to handle the communication between Jackalope and RDBMS via Doctrine DBAL.
 *
 * @license http://www.apache.org/licenses/LICENSE-2.0  Apache License Version 2.0, January 2004
 *
 * @author Benjamin Eberlei <kontakt@beberlei.de>
 */
class Client implements TransportInterface
{
    /**
     * @var Doctrine\DBAL\Connection
     */
    private $conn;

    /**
     * @var bool
     */
    private $loggedIn = false;

    /**
     * @var \PHPCR\SimpleCredentials
     */
    private $credentials;

    /**
     * @var int|string
     */
    private $workspaceId;

    /**
     * @var string
     */
    private $workspaceName;

    /**
     * @var array
     */
    private $nodeIdentifiers = array();

    /**
     *
     * @var PHPCR\NodeType\NodeTypeManagerInterface
     */
    private $nodeTypeManager = null;

    /**
     * @var array
     */
    private $fetchedUserNamespaces = false;

    /**
     * Check if an initial request on login should be send to check if repository exists
     * This is according to the JCR specifications and set to true by default
     * @see setCheckLoginOnServer
     * @var bool
     */
    private $checkLoginOnServer = true;

    /**
     * @var array
     */
    private $namespaces = array(
        \PHPCR\NamespaceRegistryInterface::PREFIX_EMPTY => \PHPCR\NamespaceRegistryInterface::NAMESPACE_EMPTY,
        \PHPCR\NamespaceRegistryInterface::PREFIX_JCR => \PHPCR\NamespaceRegistryInterface::NAMESPACE_JCR,
        \PHPCR\NamespaceRegistryInterface::PREFIX_NT => \PHPCR\NamespaceRegistryInterface::NAMESPACE_NT,
        \PHPCR\NamespaceRegistryInterface::PREFIX_MIX => \PHPCR\NamespaceRegistryInterface::NAMESPACE_MIX,
        \PHPCR\NamespaceRegistryInterface::PREFIX_XML => \PHPCR\NamespaceRegistryInterface::NAMESPACE_XML,
        'phpcr' => 'http://github.com/jackalope/jackalope', // TODO: Namespace?
    );

    /**
     * Indexes
     *
     * @var array
     */
    private $indexes;

    /**
     * @var string|null
     */
    private $sequenceWorkspaceName;
    /**
     * @var string|null
     */
    private $sequenceNodeName;
    /**
     * @var string|null
     */
    private $sequenceTypeName;

    /**
     * @var Doctrine\Common\Cache\Cache
     */
    private $cache;

    public function __construct($factory, Connection $conn, array $indexes = array(), Cache $cache = null)
    {
        $this->factory = $factory;
        $this->conn = $conn;
        $this->indexes = $indexes;
        $this->sequenceWorkspaceName = ($conn->getDatabasePlatform() instanceof \Doctrine\DBAL\Platforms\PostgreSqlPlatform) ? 'phpcr_workspaces_id_seq' : null;
        $this->sequenceNodeName = ($conn->getDatabasePlatform() instanceof \Doctrine\DBAL\Platforms\PostgreSqlPlatform) ? 'phpcr_nodes_id_seq' : null;
        $this->sequenceTypeName = ($conn->getDatabasePlatform() instanceof \Doctrine\DBAL\Platforms\PostgreSqlPlatform) ? 'phpcr_type_nodes_id_seq' : null;
        $this->cache = $cache ?: new ArrayCache();
    }

    /**
     * @return Doctrine\DBAL\Connection
     */
    public function getConnection()
    {
        return $this->conn;
    }

    // inherit all doc
    public function createWorkspace($name, $srcWorkspace = null)
    {
        if (null !== $srcWorkspace) {
            throw new \Jackalope\NotImplementedException();
        }

        $workspaceId = $this->getWorkspaceId($name);
        if ($workspaceId !== false) {
            throw new \PHPCR\RepositoryException("Workspace '" . $name . "' already exists");
        }
        $this->conn->insert('phpcr_workspaces', array('name' => $name));
        $workspaceId = $this->conn->lastInsertId($this->sequenceWorkspaceName);
        if (!$workspaceId) {
            throw new \PHPCR\RepositoryException("Workspace creation fails.");
        }

        $this->conn->insert("phpcr_nodes", array(
            'path'          => '/',
            'parent'        => '',
            'workspace_id'  => $workspaceId,
            'identifier'    => UUIDHelper::generateUUID(),
            'type'          => 'nt:unstructured',
            'local_name'    => '',
            'namespace'     => '',
            'props' => '<?xml version="1.0" encoding="UTF-8"?>
<sv:node xmlns:mix="http://www.jcp.org/jcr/mix/1.0" xmlns:nt="http://www.jcp.org/jcr/nt/1.0" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:jcr="http://www.jcp.org/jcr/1.0" xmlns:sv="http://www.jcp.org/jcr/sv/1.0" xmlns:rep="internal" />'
        ));
    }

    // inherit all doc
    public function login(\PHPCR\CredentialsInterface $credentials = null, $workspaceName = 'default')
    {
        $this->credentials = $credentials;
        $this->workspaceName = $workspaceName;

        if (!$this->checkLoginOnServer) {
            return true;
        }

        $this->workspaceId = $this->getWorkspaceId($workspaceName);
        if (!$this->workspaceId) {
            // create default workspace if it not exists
            if ($workspaceName === "default") {
                $this->createWorkspace($workspaceName);
                $this->workspaceId = $this->getWorkspaceId($workspaceName);
            }
        }

        if (!$this->workspaceId) {
            throw new \PHPCR\NoSuchWorkspaceException;
        }

        $this->loggedIn = true;
        return true;
    }

    // inherit all doc
    public function logout()
    {
        $this->loggedIn = false;
        $this->conn = null;
    }

    // inherit all doc
    public function setCheckLoginOnServer($bool)
    {
        $this->checkLoginOnServer = $bool;
    }

    private function getWorkspaceId($workspaceName)
    {
        $sql = "SELECT id FROM phpcr_workspaces WHERE name = ?";
        return $this->conn->fetchColumn($sql, array($workspaceName));
    }

    private function assertLoggedIn()
    {
        if (!$this->loggedIn) {
            if (!$this->checkLoginOnServer && $this->workspaceName) {
                $credentials = $this->credentials;
                $workspaceName = $this->workspaceName;
                $this->credentials = $this->workspaceName = null;
                $this->checkLoginOnServer = true;
                if ($this->login($credentials, $workspaceName)) {
                    return;
                }
            }

            throw new RepositoryException();
        }
    }

    // inherit all doc
    public function getRepositoryDescriptors()
    {
        return array(
          'identifier.stability' => \PHPCR\RepositoryInterface::IDENTIFIER_STABILITY_INDEFINITE_DURATION,
          'jcr.repository.name'  => 'jackalope_doctrine_dbal',
          'jcr.repository.vendor' => 'Jackalope Community',
          'jcr.repository.vendor.url' => 'http://github.com/jackalope',
          'jcr.repository.version' => '1.0.0-DEV',
          'jcr.specification.name' => 'Content Repository for PHP',
          'jcr.specification.version' => false,
          'level.1.supported' => false,
          'level.2.supported' => false,
          'node.type.management.autocreated.definitions.supported' => true,
          'node.type.management.inheritance' => true,
          'node.type.management.multiple.binary.properties.supported' => true,
          'node.type.management.multivalued.properties.supported' => true,
          'node.type.management.orderable.child.nodes.supported' => false,
          'node.type.management.overrides.supported' => false,
          'node.type.management.primary.item.name.supported' => true,
          'node.type.management.property.types' => true,
          'node.type.management.residual.definitions.supported' => false,
          'node.type.management.same.name.siblings.supported' => false,
          'node.type.management.update.in.use.suported' => false,
          'node.type.management.value.constraints.supported' => false,
          'option.access.control.supported' => false,
          'option.activities.supported' => false,
          'option.baselines.supported' => false,
          'option.journaled.observation.supported' => false,
          'option.lifecycle.supported' => false,
          'option.locking.supported' => false,
          'option.node.and.property.with.same.name.supported' => false,
          'option.node.type.management.supported' => true,
          'option.observation.supported' => false,
          'option.query.sql.supported' => false,
          'option.retention.supported' => false,
          'option.shareable.nodes.supported' => false,
          'option.simple.versioning.supported' => false,
          'option.transactions.supported' => true,
          'option.unfiled.content.supported' => true,
          'option.update.mixin.node.types.supported' => true,
          'option.update.primary.node.type.supported' => true,
          'option.versioning.supported' => false,
          'option.workspace.management.supported' => true,
          'option.xml.export.supported' => false,
          'option.xml.import.supported' => false,
          'query.full.text.search.supported' => false,
          'query.joins' => false,
          'query.languages' => '',
          'query.stored.queries.supported' => false,
          'query.xpath.doc.order' => false,
          'query.xpath.pos.index' => false,
          'write.supported' => true,
        );
    }

    // inherit all doc
    public function getNamespaces()
    {
        if ($this->fetchedUserNamespaces === false) {
            $data = $this->conn->fetchAll('SELECT * FROM phpcr_namespaces');
            $this->fetchedUserNamespaces = true;

            foreach ($data AS $row) {
                $this->namespaces[$row['prefix']] = $row['uri'];
            }
        }
        return $this->namespaces;
    }

    // inherit all doc
    public function copyNode($srcAbsPath, $dstAbsPath, $srcWorkspace = null)
    {
        $this->assertLoggedIn();

        $workspaceId = $this->workspaceId;
        if (null !== $srcWorkspace) {
            $workspaceId = $this->getWorkspaceId($srcWorkspace);
            if ($workspaceId === false) {
                throw new \PHPCR\NoSuchWorkspaceException("Source workspace '" . $srcWorkspace . "' does not exist.");
            }
        }

        if (substr($dstAbsPath, -1, 1) == "]") {
            // TODO: Understand assumptions of CopyMethodsTest::testCopyInvalidDstPath more
            throw new \PHPCR\RepositoryException("Invalid destination path");
        }

        $srcNodeId = $this->pathExists($srcAbsPath);
        if (!$srcNodeId) {
            throw new \PHPCR\PathNotFoundException("Source path '".$srcAbsPath."' not found");
        }

        if ($this->pathExists($dstAbsPath)) {
            throw new \PHPCR\ItemExistsException("Cannot copy to destination path '" . $dstAbsPath . "' that already exists.");
        }

        if (!$this->pathExists($this->getParentPath($dstAbsPath))) {
            throw new \PHPCR\PathNotFoundException("Parent of the destination path '" . $this->getParentPath($dstAbsPath) . "' has to exist.");
        }

        // Algorithm:
        // 1. Select all nodes with path $srcAbsPath."%" and iterate them
        // 2. create a new node with path $dstAbsPath + leftovers, with a new uuid. Save old => new uuid
        // 3. copy all properties from old node to new node
        // 4. if a reference is in the properties, either update the uuid based on the map if its inside the copied graph or keep it.
        // 5. "May drop mixin types"

        $this->conn->beginTransaction();

        try {

            $sql = "SELECT * FROM phpcr_nodes WHERE path LIKE ? AND workspace_id = ?";
            $stmt = $this->conn->executeQuery($sql, array($srcAbsPath . "%", $workspaceId));

            while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                $newPath = str_replace($srcAbsPath, $dstAbsPath, $row['path']);

                $dom = new \DOMDocument('1.0', 'UTF-8');
                $dom->loadXML($row['props']);

                $newNodeId = $this->syncNode(null, $newPath, $this->getParentPath($newPath), $row['type'], array(), array('dom' => $dom, 'binaryData' => array()));

                $query = "INSERT INTO phpcr_binarydata (node_id, property_name, workspace_id, idx, data) " .
                         "SELECT ?, b.property_name, ?, b.idx, b.data " .
                         "FROM phpcr_binarydata b WHERE b.node_id = ?";
                $this->conn->executeUpdate($query, array($newNodeId, $this->workspaceId, $srcNodeId));
            }
            $this->conn->commit();
        } catch (\Exception $e) {
            $this->conn->rollBack();
            throw $e;
        }
    }

    /**
     * @param string $path
     * @return array
     */
    private function getJcrName($path)
    {
        $name = implode("", array_slice(explode("/", $path), -1, 1));
        if (strpos($name, ":") === false) {
            $alias = "";
        } else {
            list($alias, $name) = explode(":", $name);
        }
        $namespaces = $this->getNamespaces();
        return array($namespaces[$alias], $name);
    }

    private function syncNode($uuid, $path, $parent, $type, $props = array(), $propsData = array())
    {
        // TODO: Not sure if there are always ALL props in $props, should be grab the online data here?
        // TODO: Binary data is handled very inefficiently here, UPSERT will really be necessary here aswell as lzy handling

        $this->conn->beginTransaction();

        try {
            if (!$propsData) {
                $propsData = $this->propsToXML($props);
            }

            if ($uuid === null) {
                $uuid = UUIDHelper::generateUUID();
            }
            $nodeId = $this->pathExists($path);
            if (!$nodeId) {
                list($namespace, $localName) = $this->getJcrName($path);
                $this->conn->insert("phpcr_nodes", array(
                    'identifier'    => $uuid,
                    'type'          => $type,
                    'path'          => $path,
                    'local_name'    => $localName,
                    'namespace'     => $namespace,
                    'parent'        => $parent,
                    'workspace_id'  => $this->workspaceId,
                    'props'         => $propsData['dom']->saveXML(),
                ));

                $nodeId = $this->conn->lastInsertId($this->sequenceNodeName);
            } else {
                $this->conn->update('phpcr_nodes', array(
                    'props' => $propsData['dom']->saveXML(),
                ), array('id' => $nodeId));
            }
            $this->nodeIdentifiers[$path] = $uuid;

            if (isset($propsData['binaryData'])) {
                $this->syncBinaryData($nodeId, $propsData['binaryData']);
            }

            // update foreign keys (references)
            $this->syncForeignKeys($nodeId, $path, $props);

            // Update internal indexes
            $this->syncInternalIndexes();
            // Update user indexes
            $this->syncUserIndexes();

            $this->conn->commit();
        } catch(\Exception $e) {
            $this->conn->rollback();
            throw $e;
        }

        return $nodeId;
    }

    private function syncInternalIndexes()
    {
        // TODO:
    }

    private function syncUserIndexes()
    {

    }

    private function syncBinaryData($nodeId, $binaryData)
    {
        foreach ($binaryData AS $propertyName => $binaryValues) {
            foreach ($binaryValues AS $idx => $data) {
                $this->conn->delete('phpcr_binarydata', array(
                    'node_id'       => $nodeId,
                    'property_name' => $propertyName,
                    'workspace_id'  => $this->workspaceId,
                ));
                $this->conn->insert('phpcr_binarydata', array(
                    'node_id'       => $nodeId,
                    'property_name' => $propertyName,
                    'workspace_id'  => $this->workspaceId,
                    'idx'           => $idx,
                    'data'          => $data,
                ));
            }
        }
    }

    private function syncForeignKeys($nodeId, $path, $props)
    {
        $this->conn->delete('phpcr_nodes_foreignkeys', array(
            'source_id' => $nodeId,
        ));
        foreach ($props AS $property) {
            $type = $property->getType();
            if ($type == \PHPCR\PropertyType::REFERENCE || $type == \PHPCR\PropertyType::WEAKREFERENCE) {
                $values = array_unique( $property->isMultiple() ? $property->getString() : array($property->getString()) );

                foreach ($values AS $value) {
                    $targetId = $this->pathExists($this->getNodePathForIdentifier($value));
                    if (!$targetId) {
                        if ($type == \PHPCR\PropertyType::REFERENCE) {
                            throw new \PHPCR\ReferentialIntegrityException(
                                "Trying to store reference to non-existant node with path '" . $value . "' in " .
                                "node " . $path . " property " . $property->getName()
                            );
                        }
                        // skip otherwise
                    } else {
                        $this->conn->insert('phpcr_nodes_foreignkeys', array(
                            'source_id' => $nodeId,
                            'source_property_name' => $property->getName(),
                            'target_id' => $targetId,
                            'type' => $type
                        ));
                    }
                }
            }
        }
    }

    /**
     * Seperate properties array into an xml and binary data.
     *
     * @param array $properties
     * @param bool $inlineBinaries
     * @return array ('dom' => $dom, 'binary' => streams)
     */
    static public function propsToXML($properties, $inlineBinaries = false)
    {
        $namespaces = array(
            'mix' => "http://www.jcp.org/jcr/mix/1.0",
            'nt' => "http://www.jcp.org/jcr/nt/1.0",
            'xs' => "http://www.w3.org/2001/XMLSchema",
            'jcr' => "http://www.jcp.org/jcr/1.0",
            'sv' => "http://www.jcp.org/jcr/sv/1.0",
            'rep' => "internal"
        );

        $dom = new \DOMDocument('1.0', 'UTF-8');
        $rootNode = $dom->createElement('sv:node');
        foreach ($namespaces as $namespace => $uri) {
            $rootNode->setAttribute('xmlns:' . $namespace, $uri);
        }
        $dom->appendChild($rootNode);

        $binaryData = null;
        foreach ($properties AS $property) {
            /* @var $prop \PHPCR\PropertyInterface */
            $propertyNode = $dom->createElement('sv:property');
            $propertyNode->setAttribute('sv:name', $property->getName());
            $propertyNode->setAttribute('sv:type', $property->getType()); // TODO: Name! not int
            $propertyNode->setAttribute('sv:multi-valued', $property->isMultiple() ? "1" : "0");

            switch ($property->getType()) {
                case \PHPCR\PropertyType::NAME:
                case \PHPCR\PropertyType::URI:
                case \PHPCR\PropertyType::WEAKREFERENCE:
                case \PHPCR\PropertyType::REFERENCE:
                case \PHPCR\PropertyType::PATH:
                case \PHPCR\PropertyType::STRING:
                    $values = $property->getString();
                    break;
                case \PHPCR\PropertyType::DECIMAL:
                    $values = $property->getDecimal();
                    break;
                case \PHPCR\PropertyType::BOOLEAN:;
                    $values = $property->getBoolean() ? "1" : "0";
                    break;
                case \PHPCR\PropertyType::LONG:
                    $values = $property->getLong();
                    break;
                case \PHPCR\PropertyType::BINARY:
                    if ($property->isMultiple()) {
                        foreach ((array)$property->getBinary() AS $binary) {
                            $binary = stream_get_contents($binary);
                            $binaryData[$property->getName()][] = $binary;
                            $values[] = strlen($binary);
                        }
                    } else {
                        $binary = stream_get_contents($property->getBinary());
                        $binaryData[$property->getName()][] = $binary;
                        $values = strlen($binary);
                    }
                    break;
                case \PHPCR\PropertyType::DATE:
                    $date = $property->getDate() ?: new \DateTime("now");
                    $values = $date->format('r');
                    break;
                case \PHPCR\PropertyType::DOUBLE:
                    $values = $property->getDouble();
                    break;
            }

            foreach ((array)$values AS $value) {
                $propertyNode->appendChild($dom->createElement('sv:value', $value));
            }

            $rootNode->appendChild($propertyNode);
        }

        return array('dom' => $dom, 'binaryData' => $binaryData);
    }

    // inherit all doc
    public function getAccessibleWorkspaceNames()
    {
        $workspaceNames = array();
        foreach ($this->conn->fetchAll("SELECT name FROM phpcr_workspaces") AS $row) {
            $workspaceNames[] = $row['name'];
        }
        return $workspaceNames;
    }

    // inherit all doc
    public function getNode($path)
    {
        $this->assertLoggedIn();

        $sql = "SELECT * FROM phpcr_nodes WHERE path = ? AND workspace_id = ?";
        $row = $this->conn->fetchAssoc($sql, array($path, $this->workspaceId));
        if (!$row) {
            throw new \PHPCR\ItemNotFoundException("Item /".$path." not found.");
        }

        $data = new \stdClass();
        // TODO: only return jcr:uuid when this node implements mix:referencable
        $data->{'jcr:uuid'} = $row['identifier'];
        $data->{'jcr:primaryType'} = $row['type'];
        $this->nodeIdentifiers[$path] = $row['identifier'];

        $sql = "SELECT path FROM phpcr_nodes WHERE parent = ? AND workspace_id = ?";
        $children = $this->conn->fetchAll($sql, array($path, $this->workspaceId));

        foreach ($children AS $child) {
            $childName = explode("/", $child['path']);
            $childName = end($childName);
            $data->{$childName} = new \stdClass();
        }

        $dom = new \DOMDocument('1.0', 'UTF-8');
        $dom->loadXML($row['props']);

        foreach ($dom->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'property') AS $propertyNode) {
            $name = $propertyNode->getAttribute('sv:name');
            $values = array();
            $type = (int)$propertyNode->getAttribute('sv:type');
            foreach ($propertyNode->childNodes AS $valueNode) {
                switch ($type) {
                    case \PHPCR\PropertyType::NAME:
                    case \PHPCR\PropertyType::URI:
                    case \PHPCR\PropertyType::WEAKREFERENCE:
                    case \PHPCR\PropertyType::REFERENCE:
                    case \PHPCR\PropertyType::PATH:
                    case \PHPCR\PropertyType::DECIMAL:
                    case \PHPCR\PropertyType::STRING:
                        $values[] = $valueNode->nodeValue;
                        break;
                    case \PHPCR\PropertyType::BOOLEAN:
                        $values[] = (bool)$valueNode->nodeValue;
                        break;
                    case \PHPCR\PropertyType::LONG:
                        $values[] = (int)$valueNode->nodeValue;
                        break;
                    case \PHPCR\PropertyType::BINARY:
                        $values[] = (int)$valueNode->nodeValue;
                        break;
                    case \PHPCR\PropertyType::DATE:
                        $values[] = $valueNode->nodeValue;
                        break;
                    case \PHPCR\PropertyType::DOUBLE:
                        $values[] = (double)$valueNode->nodeValue;
                        break;
                    default:
                        throw new \InvalidArgumentException("Type with constant " . $type . " not found.");
                }
            }

            if ($type == \PHPCR\PropertyType::BINARY) {
                if ($propertyNode->getAttribute('sv:multi-valued') == 1) {
                    $data->{":" . $name} = $values;
                } else {
                    $data->{":" . $name} = $values[0];
                }
            } else {
                if ($propertyNode->getAttribute('sv:multi-valued') == 1) {
                    $data->{$name} = $values;
                } else {
                    $data->{$name} = $values[0];
                }
                $data->{":" . $name} = $type;
            }
        }

        return $data;
    }

    // inherit all doc
    public function getNodes($paths)
    {
        $nodes = array();
        foreach ($paths as $key => $path) {
            try {
                $nodes[$key] = $this->getNode($path);
            } catch (\PHPCR\ItemNotFoundException $e) {
                // ignore
            }
        }

        return $nodes;
    }

    // inherit all doc
    public function checkinItem($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    // inherit all doc
    public function checkoutItem($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    // inherit all doc
    public function restoreItem($removeExisting, $versionPath, $path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    // inherit all doc
    public function getVersionHistory($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    private function pathExists($path)
    {
        $query = "SELECT id FROM phpcr_nodes WHERE path = ? AND workspace_id = ?";
        if ($nodeId = $this->conn->fetchColumn($query, array($path, $this->workspaceId))) {
            return $nodeId;
        }
        return false;
    }

    // inherit all doc
    public function deleteNode($path)
    {
        $this->assertLoggedIn();

        $nodeId = $this->pathExists($path);

        if (!$nodeId) {
            // This might still be a property
            $nodePath = $this->getParentPath($path);
            $nodeId = $this->pathExists($nodePath);
            if (!$nodeId) {
                // no we really don't know that path
                throw new \PHPCR\ItemNotFoundException("No item found at ".$path);
            }
            $propertyName = str_replace($nodePath, "", $path);

            $query = "SELECT props FROM phpcr_nodes WHERE id = ?";
            $xml = $this->conn->fetchColumn($query, array($nodeId));

            $dom = new \DOMDocument('1.0', 'UTF-8');
            $dom->loadXml($xml);

            foreach ($dom->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'property') AS $propertyNode) {
                if ($propertyName == $propertyNode->getAttribute('sv:name')) {
                    $propertyNode->parentNode->removeChild($propertyNode);
                    break;
                }
            }
            $xml = $dom->saveXML();

            $query = "UPDATE phpcr_nodes SET props = ? WHERE id = ?";
            $params = array($xml, $nodeId);

        } else {
            $params = array($path."%", $this->workspaceId);

            $query = "SELECT count(*) FROM phpcr_nodes_foreignkeys fk INNER JOIN phpcr_nodes n ON n.id = fk.target_id " .
                     "WHERE n.path LIKE ? AND workspace_id = ? AND fk.type = " . \PHPCR\PropertyType::REFERENCE;
            $fkReferences = $this->conn->fetchColumn($query, $params);
            if ($fkReferences > 0) {
                throw new \PHPCR\ReferentialIntegrityException("Cannot delete " . $path . ": A reference points to this node or a subnode.");
            }

            $query = "DELETE FROM phpcr_nodes WHERE path LIKE ? AND workspace_id = ?";
        }

        $this->conn->beginTransaction();

        try {
            $this->conn->executeUpdate($query, $params);
            $this->conn->commit();

            return true;
        } catch(\Exception $e) {
            $this->conn->rollBack();
            return false;
        }
    }

    // inherit all doc
    public function deleteProperty($path)
    {
        // TODO:
    }

    // inherit all doc
    public function moveNode($srcAbsPath, $dstAbsPath)
    {
        $this->assertLoggedIn();

        throw new \Jackalope\NotImplementedException("Moving nodes is not yet implemented");
    }

    /**
     * Get parent path of a path.
     *
     * @param string $path
     * @return string
     */
    private function getParentPath($path)
    {
        $parent = implode("/", array_slice(explode("/", $path), 0, -1));
        if (!$parent) {
            return "/";
        }
        return $parent;
    }

    /**
     * @param \PHPCR\NodeInterface $node
     * @param \PHPCR\NodeType\NodeTypeDefinitionInterface $def
     */
    private function validateNode($node, $def)
    {
        foreach ($def->getDeclaredChildNodeDefinitions() AS $childDef) {
            /* @var $childDef \PHPCR\NodeType\NodeDefinitionInterface */
            if (!$node->hasNode($childDef->getName())) {
                if ($childDef->getName() === '*') {
                    continue;
                }

                if ($childDef->isMandatory() && !$childDef->isAutoCreated()) {
                    throw new \PHPCR\RepositoryException(
                        "Child " . $child->getName() . " is mandatory, but is not present while ".
                        "saving " . $def->getName() . " at " . $node->getPath()
                    );
                } elseif ($childDef->isAutoCreated()) {
                    throw new \Jackalope\NotImplementedException("Auto-creation of child node '".$def->getName()."#".$childDef->getName()."' is not yet supported in DoctrineDBAL transport.");
                }

                if ($node->hasProperty($childDef->getName())) {
                    throw new \PHPCR\RepositoryException(
                        "Node " . $node->getPath() . " has property with name ".
                        $childDef->getName() . " but its node type '". $def->getName() . "' defines a ".
                        "child with this name."
                    );
                }
            }
        }

        foreach ($def->getDeclaredPropertyDefinitions() AS $propertyDef) {
            /* @var $propertyDef \PHPCR\NodeType\PropertyDefinitionInterface */
            if ($propertyDef->getName() == '*') {
                continue;
            }

            if (!$node->hasProperty($propertyDef->getName())) {
                if ($node->hasNode($propertyDef->getName())) {
                    throw new \PHPCR\RepositoryException(
                        "Node " . $node->getPath() . " has child with name ".
                        $propertyDef->getName() . " but its node type '". $def->getName() . "' defines a ".
                        "property with this name."
                    );
                }

                if ($propertyDef->isMandatory() && !$propertyDef->isAutoCreated()) {
                    throw new \PHPCR\RepositoryException(
                        "Property " . $propertyDef->getName() . " is mandatory, but is not present while ".
                        "saving " . $def->getName() . " at " . $node->getPath()
                    );
                } elseif ($propertyDef->isAutoCreated()) {
                    $defaultValues = $propertyDef->getDefaultValues();
                    $node->setProperty(
                        $propertyDef->getName(),
                        $propertyDef->isMultiple() ? $defaultValues : (isset($defaultValues[0]) ? $defaultValues[0] : null),
                        $propertyDef->getRequiredType()
                    );
                }
            }
        }
    }

    private function getResponsibleNodeTypes($node)
    {
        // This is very slow i believe :-(
        $nodeDef = $node->getPrimaryNodeType();
        $nodeTypes = $node->getMixinNodeTypes();
        array_unshift($nodeTypes, $nodeDef);
        foreach ($nodeTypes as $nodeType) {
            /* @var $nodeType \PHPCR\NodeType\NodeTypeDefinitionInterface */
            foreach ($nodeType->getDeclaredSupertypes() AS $superType) {
                $nodeTypes[] = $superType;
            }
        }
        return $nodeTypes;
    }

    // inherit all doc
    public function storeNode(\PHPCR\NodeInterface $node)
    {
        $path = $node->getPath();
        $this->assertLoggedIn();

        $nodeTypes = $this->getResponsibleNodeTypes($node);
        $popertyDefs = array();
        foreach ($nodeTypes AS $nodeType) {
            /* @var $nodeType \PHPCR\NodeType\NodeTypeDefinitionInterface */
            $this->validateNode($node, $nodeType);
        }

        $properties = $node->getProperties();

        $nodeIdentifier = (isset($properties['jcr:uuid'])) ? $properties['jcr:uuid']->getValue() : UUIDHelper::generateUUID();
        $type = isset($properties['jcr:primaryType']) ? $properties['jcr:primaryType']->getValue() : "nt:unstructured";
        $this->syncNode($nodeIdentifier, $path, $this->getParentPath($path), $type, $properties);

        return true;
    }

    // inherit all doc
    public function storeProperty(\PHPCR\PropertyInterface $property)
    {
        $this->assertLoggedIn();

        $node = $property->getParent();
        $this->storeNode($node);
        return true;
    }

    /**
     * Validation if all the data is correct before writing it into the database.
     *
     * @param int $type
     * @param mixed $value
     * @param string $path
     * @throws \PHPCR\ValueFormatException
     * @return void
     */
    private function assertValidPropertyValue($type, $value, $path)
    {
        if ($type === \PHPCR\PropertyType::NAME) {
            if (strpos($value, ":") !== false) {
                list($prefix, $localName) = explode(":", $value);

                $this->getNamespaces();
                if (!isset($this->namespaces[$prefix])) {
                    throw new \PHPCR\ValueFormatException("Invalid PHPCR NAME at " . $path . ": The namespace prefix " . $prefix . " does not exist.");
                }
            }
        } elseif ($type === \PHPCR\PropertyType::PATH) {
            if (!preg_match('((/[a-zA-Z0-9:_-]+)+)', $value)) {
                throw new \PHPCR\ValueFormatException("Invalid PATH at " . $path .": Segments are seperated by / and allowed chars are a-zA-Z0-9:_-");
            }
        } elseif ($type === \PHPCR\PropertyType::URI) {
            if (!preg_match(self::VALIDATE_URI_RFC3986, $value)) {
                throw new \PHPCR\ValueFormatException("Invalid URI at " . $path .": Has to follow RFC 3986.");
            }
        }
    }

    const VALIDATE_URI_RFC3986 = "
/^
([a-z][a-z0-9\*\-\.]*):\/\/
(?:
  (?:(?:[\w\.\-\+!$&'\(\)*\+,;=]|%[0-9a-f]{2})+:)*
  (?:[\w\.\-\+%!$&'\(\)*\+,;=]|%[0-9a-f]{2})+@
)?
(?:
  (?:[a-z0-9\-\.]|%[0-9a-f]{2})+
  |(?:\[(?:[0-9a-f]{0,4}:)*(?:[0-9a-f]{0,4})\])
)
(?::[0-9]+)?
(?:[\/|\?]
  (?:[\w#!:\.\?\+=&@!$'~*,;\/\(\)\[\]\-]|%[0-9a-f]{2})
*)?
$/xi";

    // inherit all doc
    public function getNodePathForIdentifier($uuid)
    {
        $this->assertLoggedIn();

        $path = $this->conn->fetchColumn("SELECT path FROM phpcr_nodes WHERE identifier = ? AND workspace_id = ?", array($uuid, $this->workspaceId));
        if (!$path) {
            throw new \PHPCR\ItemNotFoundException("no item found with uuid ".$uuid);
        }
        return $path;
    }

    // inherit all doc
    public function getNodeTypes($nodeTypes = array())
    {
        $nodeTypes = array_flip($nodeTypes);

        $data = PHPCR2StandardNodeTypes::getNodeTypeData();
        $filteredData = array();
        foreach ($data AS $nodeTypeData) {
            if (isset($nodeTypes[$nodeTypeData['name']])) {
                $filteredData[$nodeTypeData['name']] = $nodeTypeData;
            }
        }

        foreach ($nodeTypes AS $type => $val) {
            if (!isset($filteredData[$type]) && $result = $this->fetchUserNodeType($type)) {
                $filteredData[$type] = $result;
            }
        }

        return array_values($filteredData);
    }

    /**
     * Fetch a user-defined node-type definition.
     *
     * @param string $name
     * @return array
     */
    private function fetchUserNodeType($name)
    {
        if ($result = $this->cache->fetch('phpcr_nodetype_' . $name)) {
            return $result;
        }

        $query = "SELECT * FROM phpcr_type_nodes WHERE name = ?";
        $data = $this->conn->fetchAssoc($query, array($name));

        if (!$data) {
            $this->cache->save('phpcr_nodetype_' . $name, false);
            return false;
        }

        $result = array(
            'name' => $data['name'],
            'isAbstract' => (bool)$data['is_abstract'],
            'isMixin' => (bool)($data['is_mixin']),
            'isQueryable' => (bool)$data['is_queryable'],
            'hasOrderableChildNodes' => (bool)$data['orderable_child_nodes'],
            'primaryItemName' => $data['primary_item'],
            'declaredSuperTypeNames' => array_filter(explode(' ', $data['supertypes'])),
            'declaredPropertyDefinitions' => array(),
            'declaredNodeDefinitions' => array(),
        );

        $query = "SELECT * FROM phpcr_type_props WHERE node_type_id = ?";
        $props = $this->conn->fetchAll($query, array($data['node_type_id']));

        foreach ($props AS $propertyData) {
            $result['declaredPropertyDefinitions'][] = array(
                'declaringNodeType' => $data['name'],
                'name' => $propertyData['name'],
                'isAutoCreated' => (bool)$propertyData['auto_created'],
                'isMandatory' => (bool)$propertyData['mandatory'],
                'isProtected' => (bool)$propertyData['protected'],
                'onParentVersion' => $propertyData['on_parent_version'],
                'requiredType' => $propertyData['required_type'],
                'multiple' => (bool)$propertyData['multiple'],
                'isFulltextSearchable' => (bool)$propertyData['fulltext_searchable'],
                'isQueryOrderable' => (bool)$propertyData['query_orderable'],
                'queryOperators' => array (
                  0 => 'jcr.operator.equal.to',
                  1 => 'jcr.operator.not.equal.to',
                  2 => 'jcr.operator.greater.than',
                  3 => 'jcr.operator.greater.than.or.equal.to',
                  4 => 'jcr.operator.less.than',
                  5 => 'jcr.operator.less.than.or.equal.to',
                  6 => 'jcr.operator.like',
                ),
                'defaultValue' => array($propertyData['default_value']),
            );
        }

        $query = "SELECT * FROM phpcr_type_childs WHERE node_type_id = ?";
        $childs = $this->conn->fetchAll($query, array($data['node_type_id']));

        foreach ($childs AS $childData) {
            $result['declaredNodeDefinitions'][] = array(
                'declaringNodeType' => $data['name'],
                'name' => $childData['name'],
                'isAutoCreated' => (bool)$childData['auto_created'],
                'isMandatory' => (bool)$childData['mandatory'],
                'isProtected' => (bool)$childData['protected'],
                'onParentVersion' => $childData['on_parent_version'],
                'allowsSameNameSiblings' => false,
                'defaultPrimaryTypeName' => $childData['default_type'],
                'requiredPrimaryTypeNames' => array_filter(explode(" ", $childData['primary_types'])),
            );
        }

        $this->cache->save('phpcr_nodetype_' . $name, $result);

        return $result;
    }

    // inherit all doc
    public function registerNodeTypesCnd($cnd, $allowUpdate)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    // inherit all doc
    public function registerNodeTypes($types, $allowUpdate)
    {
        foreach ($types AS $type) {
            /* @var $type \Jackalope\NodeType\NodeTypeDefinition */
            $this->conn->insert('phpcr_type_nodes', array(
                'name' => $type->getName(),
                'supertypes' => implode(' ', $type->getDeclaredSuperTypeNames()),
                'is_abstract' => $type->isAbstract() ? 1 : 0,
                'is_mixin' => $type->isMixin() ? 1 : 0,
                'queryable' => $type->isQueryable() ? 1 : 0,
                'orderable_child_nodes' => $type->hasOrderableChildNodes() ? 1 : 0,
                'primary_item' => $type->getPrimaryItemName(),
            ));
            $nodeTypeId = $this->conn->lastInsertId($this->sequenceTypeName);

            if ($propDefs = $type->getDeclaredPropertyDefinitions()) {
                foreach ($propDefs AS $propertyDef) {
                    /* @var $propertyDef \Jackalope\NodeType\PropertyDefinition */
                    $this->conn->insert('phpcr_type_props', array(
                        'node_type_id' => $nodeTypeId,
                        'name' => $propertyDef->getName(),
                        'protected' => $propertyDef->isProtected(),
                        'mandatory' => $propertyDef->isMandatory(),
                        'auto_created' => $propertyDef->isAutoCreated(),
                        'on_parent_version' => $propertyDef->getOnParentVersion(),
                        'multiple' => $propertyDef->isMultiple(),
                        'fulltext_searchable' => $propertyDef->isFullTextSearchable(),
                        'query_orderable' => $propertyDef->isQueryOrderable(),
                        'required_type' => $propertyDef->getRequiredType(),
                        'query_operators' => 0, // transform to bitmask
                        'default_value' => $propertyDef->getDefaultValues() ? current($propertyDef->getDefaultValues()) : null,
                    ));
                }
            }

            if ($childDefs = $type->getDeclaredChildNodeDefinitions()) {
                foreach ($childDefs AS $childDef) {
                    /* @var $propertyDef \PHPCR\NodeType\NodeDefinitionInterface */
                    $this->conn->insert('phpcr_type_childs', array(
                        'node_type_id' => $nodeTypeId,
                        'name' => $childDef->getName(),
                        'protected' => $childDef->isProtected(),
                        'mandatory' => $childDef->isMandatory(),
                        'auto_created' => $childDef->isAutoCreated(),
                        'on_parent_version' => $childDef->getOnParentVersion(),
                        'primary_types' => implode(' ', $childDef->getRequiredPrimaryTypeNames() ?: array()),
                        'default_type' => $childDef->getDefaultPrimaryTypeName(),
                    ));
                }
            }
        }
    }

    // inherit all doc
    public function setNodeTypeManager($nodeTypeManager)
    {
        $this->nodeTypeManager = $nodeTypeManager;
    }

    // inherit all doc
    public function cloneFrom($srcWorkspace, $srcAbsPath, $destAbsPath, $removeExisting)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    // inherit all doc
    public function getBinaryStream($path)
    {
        $this->assertLoggedIn();

        $nodePath = $this->getParentPath($path);
        $propertyName = ltrim(str_replace($nodePath, "", $path), "/"); // i dont know why trim here :/
        $nodeId = $this->pathExists($nodePath);

        $data = $this->conn->fetchAll(
            'SELECT data, idx FROM phpcr_binarydata WHERE node_id = ? AND property_name = ? AND workspace_id = ?',
            array($nodeId, $propertyName, $this->workspaceId)
        );

        // TODO: Error Handling on the stream?
        if (count($data) == 1) {
            return fopen("data://text/plain,".$data[0]['data'], "r");
        } else {
            $streams = array();
            foreach ($data AS $row) {
                $streams[$row['idx']] = fopen("data://text/plain,".$row['data'], "r");
            }
            return $streams;
        }
    }

    // inherit all doc
    public function getProperty($path)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    // inherit all doc
    public function query(\PHPCR\Query\QueryInterface $query)
    {
        $this->assertLoggedIn();

        $limit = $query->getLimit();
        $offset = $query->getOffset();

        switch ($query->getLanguage()) {
            case \PHPCR\Query\QueryInterface::JCR_SQL2:
                $parser = new \PHPCR\Util\QOM\Sql2ToQomQueryConverter(new \Jackalope\Query\QOM\QueryObjectModelFactory());
                $qom = $parser->parse($query->getStatement());

                $qomWalker = new Query\QOMWalker($this->nodeTypeManager, $this->conn->getDatabasePlatform(), $this->getNamespaces());
                $sql = $qomWalker->walkQOMQuery($qom);

                $sql = $this->conn->getDatabasePlatform()->modifyLimitQuery($sql, $limit, $offset);
                $data = $this->conn->fetchAll($sql, array($this->workspaceId));

                $result = array();
                foreach ($data AS $row) {
                    $result[] = array(
                        array('dcr:name' => 'jcr:primaryType', 'dcr:value' => $row['type']),
                        array('dcr:name' => 'jcr:path', 'dcr:value' => $row['path'], 'dcr:selectorName' => $row['type']),
                        array('dcr:name' => 'jcr:score', 'dcr:value' => 0)
                    );
                }

                return $result;
            case \PHPCR\Query\QueryInterface::JCR_JQOM:
                // How do we extrct the QOM from a QueryInterface? We need a non-interface method probably
                throw new \Jackalope\NotImplementedException("JCQ-JQOM not yet implemented.");
                break;
        }
    }

    // inherit all doc
    public function registerNamespace($prefix, $uri)
    {
        $this->conn->insert('phpcr_namespaces', array(
            'prefix' => $prefix,
            'uri' => $uri,
        ));
    }

    // inherit all doc
    public function unregisterNamespace($prefix)
    {
        $this->conn->delete('phpcr_namespaces', array('prefix' => $prefix));
    }

    // inherit all doc
    public function getReferences($path, $name = null)
    {
        return $this->getNodeReferences($path, $name, false);
    }

    // inherit all doc
    public function getWeakReferences($path, $name = null)
    {
        return $this->getNodeReferences($path, $name, true);
    }

    // inherit all doc
    protected function getNodeReferences($path, $name = null, $weakReference = false)
    {
        $targetId = $this->pathExists($path);

        $type = $weakReference ? \PHPCR\PropertyType::WEAKREFERENCE : \PHPCR\PropertyType::REFERENCE;

        $sql = "SELECT CONCAT(n.path, '/', fk.source_property_name) AS path, fk.source_property_name FROM phpcr_nodes n " .
               "INNER JOIN phpcr_nodes_foreignkeys fk ON n.id = fk.source_id ".
               "WHERE fk.target_id = ? AND fk.type = ?";
        $properties = $this->conn->fetchAll($sql, array($targetId, $type));

        $references = array();
        foreach ($properties AS $property) {
            if ($name === null || $property['source_property_name'] == $name) {
                $references[] = $property['path'];
            }
        }
        return $references;
    }

    // inherit all doc
    public function getPermissions($path)
    {
        return array(
            \PHPCR\SessionInterface::ACTION_ADD_NODE,
            \PHPCR\SessionInterface::ACTION_READ,
            \PHPCR\SessionInterface::ACTION_REMOVE,
            \PHPCR\SessionInterface::ACTION_SET_PROPERTY);
    }

    private function assertValidPath($path)
    {
        if (! (strpos($path, '//') === false
              && strpos($path, '/../') === false
              && preg_match('/^[\w{}\/#:^+~*\[\]\. -]*$/i', $path))
        ) {
            throw new \PHPCR\RepositoryException('Path is not well-formed or contains invalid characters: ' . $path);
        }
    }
}
