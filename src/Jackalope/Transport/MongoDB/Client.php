<?php

/**
 * Class to handle the communication between Jackalope and MongoDB.
 *
 * @license http://www.apache.org/licenses/LICENSE-2.0  Apache License Version 2.0, January 2004
 *   Licensed under the Apache License, Version 2.0 (the "License") {}
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * @package jackalope
 * @subpackage transport
 */

namespace Jackalope\Transport\MongoDB;

use PHPCR\PropertyType;
use PHPCR\RepositoryException;
use PHPCR\Util\UUIDHelper;
use Jackalope\TransportInterface;
use Jackalope\Helper;
use Jackalope\NodeType\NodeTypeManager;
use Jackalope\NodeType\PHPCR2StandardNodeTypes;
use Doctrine\MongoDb\Connection;
use Doctrine\MongoDb\Database;

/**
 * @author Thomas Schedler <thomas@chirimoya.at>
 */
class Client implements TransportInterface
{
    
    /**
     * Name of MongoDB workspace collection.
     * 
     * @var string
     */
    const COLLNAME_WORKSPACES = 'phpcr_workspaces';
    
    /**
     * Name of MongoDB namespace collection.
     * 
     * @var string
     */
    const COLLNAME_NAMESPACES = 'phpcr_namespaces';
    
    /**
     * Name of MongoDB node collection.
     * 
     * @var string
     */
    const COLLNAME_NODES = 'phpcr_nodes';
    
    /**
     * @var Doctrine\MongoDB\Database
     */
    private $db;

    /**
     * @var bool
     */
    private $loggedIn = false;

    /**
     * @var int|string
     */
    private $workspaceId;

    /**
     * @var array
     */
    private $nodeTypes = array(
        "nt:file" => array(
            "is_abstract" => false,
            "properties" => array(
                "jcr:primaryType" => array('multi_valued' => false),
                "jcr:mixinTypes" => array('multi_valued' => true),
            ),
        ),
        "nt:folder" => array(
            "is_abstract" => false,
            "properties" => array(
                "jcr:primaryType" => array('multi_valued' => false),
                "jcr:mixinTypes" => array('multi_valued' => true),
            ),
        ),
    );

    /**
     * @var PHPCR\NodeType\NodeTypeManagerInterface
     */
    private $nodeTypeManager = null;

    /**
     * @var array
     */
    private $userNamespaces = null;

    /**
     * @var array
     */
    private $validNamespacePrefixes = array(
        \PHPCR\NamespaceRegistryInterface::PREFIX_EMPTY => true,
        \PHPCR\NamespaceRegistryInterface::PREFIX_JCR   => true,
        \PHPCR\NamespaceRegistryInterface::PREFIX_NT    => true,
        \PHPCR\NamespaceRegistryInterface::PREFIX_MIX   => true,
        \PHPCR\NamespaceRegistryInterface::PREFIX_XML   => true,
    );
    
    /**
     * Create a transport pointing to a server url.
     *
     * @param object $factory  an object factory implementing "get" as described in \Jackalope\Factory.
     * @param Doctrine\MongoDB\Database $db
     */
    public function __construct($factory, Database $db)
    {
        $this->factory = $factory;
        $this->db = $db;
    }

    /**
     * Creates a new Workspace with the specified name. The new workspace is
     * empty, meaning it contains only root node.
     *
     * If srcWorkspace is given:
     * Creates a new Workspace with the specified name initialized with a
     * clone of the content of the workspace srcWorkspace. Semantically,
     * this method is equivalent to creating a new workspace and manually
     * cloning srcWorkspace to it; however, this method may assist some
     * implementations in optimizing subsequent Node.update and Node.merge
     * calls between the new workspace and its source.
     *
     * The new workspace can be accessed through a login specifying its name.
     *
     * @param string $name A String, the name of the new workspace.
     * @param string $srcWorkspace The name of the workspace from which the new workspace is to be cloned.
     * @return void
     * @throws \PHPCR\AccessDeniedException if the session through which this Workspace object was acquired does not have sufficient access to create the new workspace.
     * @throws \PHPCR\UnsupportedRepositoryOperationException if the repository does not support the creation of workspaces.
     * @throws \PHPCR\NoSuchWorkspaceException if $srcWorkspace does not exist.
     * @throws \PHPCR\RepositoryException if another error occurs.
     * @api
     */
    public function createWorkspace($name, $srcWorkspace = null)
    {
        if ($srcWorkspace !== null) {
            throw new \Jackalope\NotImplementedException();
        }
        
        $workspaceId = $this->getWorkspaceId($workspaceName);
        if ($workspaceId !== false) {
            throw new \PHPCR\RepositoryException("Workspace '" . $workspaceName . "' already exists");
        }
        
        $coll = $this->db->selectCollection(self::COLLNAME_WORKSPACES);
        $workspace = array(
            'name' => $workspaceName
        );
        $coll->insert($workspace);
        
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        $rootNode = array(
            '_id' => new \MongoBinData(UUIDHelper::generateUUID(), \MongoBinData::UUID),
            'path' => '/',
            'parent' => '-1',
            'w_id' => $workspace['_id'],
            'type' => 'nt:unstructured',
            'props' => new stdClass()
        );
        $coll->insert($rootNode);
    }

    /**
     * Set this transport to a specific credential and a workspace.
     *
     * This can only be called once. To connect to another workspace or with
     * another credential, use a fresh instance of transport.
     *
     * @param credentials A \PHPCR\SimpleCredentials instance (this is the only type currently understood)
     * @param workspaceName The workspace name for this transport.
     * @return true on success (exceptions on failure)
     *
     * @throws \PHPCR\LoginException if authentication or authorization (for the specified workspace) fails
     * @throws \PHPCR\NoSuchWorkspacexception if the specified workspaceName is not recognized
     * @throws \PHPCR\RepositoryException if another error occurs
     */
    public function login(\PHPCR\CredentialsInterface $credentials, $workspaceName)
    {
        $this->workspaceId = $this->getWorkspaceId($workspaceName);
        if (!$this->workspaceId) {
            throw new \PHPCR\NoSuchWorkspaceException;
        }

        $this->loggedIn = true;
        return true;
    }
    
    /**
     * Releases all resources associated with this Session.
     *
     * This method should be called when a Session is no longer needed.
     *
     * @return void
     */
    public function logout()
    {
        $this->loggedIn = false;
    }

    /**
     * Get workspace Id.
     * 
     * @param string $workspaceName
     * @return string|bool
     */
    private function getWorkspaceId($workspaceName)
    {
        $coll = $this->db->selectCollection(self::COLLNAME_WORKSPACES);
        
        $qb = $coll->createQueryBuilder()
                   ->field('name')->equals($workspaceName);
        
        $query = $qb->getQuery();
        $workspace = $query->getSingleResult();

        return ($workspace != null) ? $workspace['_id'] : false;
    }

    /**
     * Assert logged in.
     * 
     * @return void
     * 
     * @throws \PHPCR\RepositoryException if not logged in 
     */
    private function assertLoggedIn()
    {
        if (!$this->loggedIn) {
            throw new RepositoryException();
        }
    }

    /**
     * Get the repository descriptors from the jackrabbit server
     * This happens without login or accessing a specific workspace.
     *
     * @return Array with name => Value for the descriptors
     * 
     * @throws \PHPCR\RepositoryException if error occurs
     */
    public function getRepositoryDescriptors()
    {
        return array(); //TODO
    }

    /**
     * Get the registered namespaces mappings from the backend.
     *
     * @return array Associative array of prefix => uri
     * 
     * @throws \PHPCR\RepositoryException if now logged in
     */
    public function getNamespaces()
    {
        if ($this->userNamespaces === null) {
            $coll = $this->db->selectCollection(self::COLLNAME_NAMESPACES);
            
            $namespaces = $coll->find();
            $this->userNamespaces = array();
            
            foreach ($namespaces AS $namespace) {
                $this->validNamespacePrefixes[$namespace['prefix']] = true;
                $this->userNamespaces[$namespace['prefix']] = $namespace['uri'];
            }
        }
        return $this->userNamespaces;
    }

    /**
     * Copies a Node from src to dst
     *
     * @param   string  $srcAbsPath     Absolute source path to the node
     * @param   string  $dstAbsPath     Absolute destination path (must include the new node name)
     * @param   string  $srcWorkspace   The source workspace where the node can be found or NULL for current
     * @return void
     * 
     * @throws \PHPCR\NoSuchWorkspaceException if source workspace doesn't exist
     * @throws \PHPCR\RepositoryException if destination path is invalid
     * @throws \PHPCR\PathNotFoundException if source path is not found
     * @throws \PHPCR\ItemExistsException if destination path already exists
     *
     * @link http://www.ietf.org/rfc/rfc2518.txt
     * @see \Jackalope\Workspace::copy
     */
    public function copyNode($srcAbsPath, $dstAbsPath, $srcWorkspace = null)
    {
        $this->assertLoggedIn();
        
        $srcAbsPath = $this->validatePath($srcAbsPath);
        $dstAbsPath = $this->validatePath($dstAbsPath);

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

        if (!$this->pathExists($srcAbsPath)) {
            throw new \PHPCR\PathNotFoundException("Source path '".$srcAbsPath."' not found");
        }

        if ($this->pathExists($dstAbsPath)) {
            throw new \PHPCR\ItemExistsException("Cannot copy to destination path '" . $dstAbsPath . "' that already exists.");
        }

        if (!$this->pathExists($this->getParentPath($dstAbsPath))) {
            throw new \PHPCR\PathNotFoundException("Parent of the destination path '" . $this->getParentPath($dstAbsPath) . "' has to exist.");
        }
        
        try {

            $regex = new \MongoRegex('/^' . addcslashes($srcAbsPath, '/') . '/'); 
            
            $coll = $this->db->selectCollection(self::COLLNAME_NODES);
            $qb = $coll->createQueryBuilder()
                       ->field('path')->equals($regex)
                       ->field('w_id')->equals($workspaceId);
    
            $query = $qb->getQuery();
            $nodes = $query->getIterator();
            
            foreach ($nodes as $node){
                $newPath = str_replace($srcAbsPath, $dstAbsPath, $node['path']);
                $uuid = UUIDHelper::generateUUID();
                
                $node['_id'] = new \MongoBinData($uuid, \MongoBinData::UUID);
                $node['path'] = $newPath;
                $node['parent'] = $this->getParentPath($newPath);
                $node['w_id'] = $this->workspaceId;
                
                $coll->insert($node);
            }
            
        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * Returns the accessible workspace names
     *
     * @return array Set of workspaces to work on.
     */
    public function getAccessibleWorkspaceNames()
    {
        $coll = $this->db->selectCollection(self::COLLNAME_WORKSPACES);
        
        $workspaces = $coll->find();
        
        $workspaceNames = array();
        foreach ($workspaces AS $workspace) {
            $workspaceNames[] = $workspace['name'];
        }
        return $workspaceNames;
    }

    /**
     * Get the node from an absolute path
     *
     * Returns a json_decode stdClass structure that contains two fields for
     * each property and one field for each child.
     * A child is just containing an empty class as value (in the future we
     * could use this for eager loading with recursive structure).
     * A property consists of a field named as the property is and a value that
     * is the property value, plus a second field with the same name but
     * prefixed with a colon that has a type specified as value (out of the
     * string constants from PropertyType)
     *
     * For binary properties, the value of the type declaration is not the type
     * but the length of the binary, thus integer instead of string.
     * There is no value field for binary data (to avoid loading large amount
     * of unneeded data)
     * Use getBinaryStream to get the actual data of a binary property.
     *
     * There is a couple of "magic" properties:
     * <ul>
     *   <li>jcr:uuid - the unique id of the node</li>
     *   <li>jcr:primaryType - name of the primary type</li>
     *   <li>jcr:mixinTypes - comma separated list of mixin types</li>
     *   <li>jcr:index - the index of same name siblings</li>
     * </ul>
     *
     * @example Return struct
     * <code>
     * object(stdClass)#244 (4) {
     *      ["jcr:uuid"]=>
     *          string(36) "64605997-e298-4334-a03e-673fc1de0911"
     *      [":jcr:primaryType"]=>
     *          string(4) "Name"
     *      ["jcr:primaryType"]=>
     *          string(8) "nt:unstructured"
     *      ["myProperty"]=>
     *          string(4) "test"
     *      [":myProperty"]=>
     *          string(5) "String" //one of \PHPCR\PropertyTypeInterface::TYPENAME_NAME
     *      [":myBinary"]=>
     *          int 1538    //length of binary file, no "myBinary" field present
     *      ["childNodeName"]=>
     *          object(stdClass)#152 (0) {}
     *      ["otherChild"]=>
     *          object(stdClass)#153 (0) {}
     * }
     * </code>
     *
     * Note: the reason to use json_decode with associative = false is that the
     * array version can not distinguish between
     *   ['foo', 'bar'] and {0: 'foo', 1: 'bar'}
     * The first are properties, but the later is a list of children nodes.
     *
     * @param string $path Absolute path to the node.
     * @return array associative array for the node (decoded from json with associative = true)
     *
     * @throws \PHPCR\ItemNotFoundException If the item at path was not found
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function getNode($path)
    {
        $this->assertLoggedIn();
        $path = $this->validatePath($path);
        
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        $qb = $coll->createQueryBuilder()
                   ->field('path')->equals($path)
                   ->field('w_id')->equals($this->workspaceId);

        $query = $qb->getQuery();
        $node = $query->getSingleResult();
        
        if (!$node) {
            throw new \PHPCR\ItemNotFoundException("Item ".$path." not found.");
        }

        $data = new \stdClass();
        
        if($node['_id'] instanceof \MongoBinData) {
            $data->{'jcr:uuid'} = $node['_id']->bin;
        }
        $data->{'jcr:primaryType'} = $node['type'];
        
        //TODO prepare properties?
        foreach ($node['props'] as $name => $prop) {
            $type = $prop['type'];
            
            if ($type == \PHPCR\PropertyType::TYPENAME_BINARY) {
                if (isset($prop['multi']) && $prop['multi'] == true) {
                    foreach ($prop['value'] as $value) {
                        $data->{":" . $name}[] = $value;
                    }
                } else {
                    $data->{":" . $name} = $prop['value'];
                }
            } else if ($type == \PHPCR\PropertyType::TYPENAME_DATE) {
                if (isset($prop['multi']) && $prop['multi'] == true) {
                    foreach ($prop['value'] as $value) {
                        $date = new \DateTime(date('Y-m-d H:i:s', $value['date']->sec), new \DateTimeZone($value['timezone']));
                        $data->{$name}[] = $date->format('c');
                    }
                } else {
                    $date = new \DateTime(date('Y-m-d H:i:s', $prop['value']['date']->sec), new \DateTimeZone($prop['value']['timezone']));
                    $data->{$name} = $date->format('c');
                }
                
                $data->{":" . $name} = $type;
            } else {
                if (isset($prop['multi']) && $prop['multi'] == true) {
                    foreach ($prop['value'] as $value) {
                        $data->{$name}[] = $value;    
                    }
                } else {
                    $data->{$name} = $prop['value'];
                }
                $data->{":" . $name} = $type;
            }
        }
        
        $qb = $coll->createQueryBuilder()
                   ->field('parent')->equals($path)
                   ->field('w_id')->equals($this->workspaceId);

        $query = $qb->getQuery();
        $children = $query->getIterator();
        
        foreach ($children AS $child) {
            $childName = explode("/", $child['path']);
            $childName = end($childName);
            $data->{$childName} = new \stdClass();
        }

        return $data;
    }
    
    /**
     * Get the nodes from an array of absolute paths
     *
     * @param array $path Absolute paths to the nodes.
     * @return array associative array for the node (decoded from json with associative = true)
     *
     * @throws \PHPCR\RepositoryException if not logged in
     */
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

    /**
     * Check-in item at path.
     *
     * @param string $path
     * @return string
     *
     * @throws PHPCR\UnsupportedRepositoryOperationException
     * @throws PHPCR\RepositoryException
     */
    public function checkinItem($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    /**
     * Check-out item at path.
     *
     * @param string $path
     * @return void
     *
     * @throws PHPCR\UnsupportedRepositoryOperationException
     * @throws PHPCR\RepositoryException
     */
    public function checkoutItem($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    public function restoreItem($removeExisting, $versionPath, $path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    public function getVersionHistory($path)
    {
        throw new \Jackalope\NotImplementedException();
    }

    public function querySQL($query, $limit = null, $offset = null)
    {
        throw new \Jackalope\NotImplementedException();
    }

    /**
     * Checks if path exists.
     * 
     * @param string $path
     * @return bool
     */
    private function pathExists($path)
    {
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        
        $qb = $coll->createQueryBuilder()
                   ->field('path')->equals($path)
                   ->field('w_id')->equals($this->workspaceId);
        
        $query = $qb->getQuery();
        
        if (!$query->getSingleResult()) {
            return false;
        }
        return true;
    }

    /**
     * Deletes a node and its subnodes
     *
     * @param string $path Absolute path to identify a special item.
     * @return bool true on success
     *
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function deleteNode($path)
    {
        $path = $this->validatePath($path);
        $this->assertLoggedIn();

        //TODO check if there is a node with a reference to this or a subnode of the path
        /*
        $match = $path."%";
        $query = "SELECT node_identifier FROM jcrprops WHERE type = ? AND string_data LIKE ? AND w_id = ?";
        if ($ident = $this->conn->fetchColumn($query, array(\PHPCR\PropertyType::REFERENCE, $match, $this->workspaceId))) {
            throw new \PHPCR\ReferentialIntegrityException(
                "Cannot delete item at path '".$path."', there is at least one item (ident ".$ident.") with ".
                "a reference to this or a subnode of the path."
            );
        }*/

        if (!$this->pathExists($path)) {
            $this->deleteProperty($path); //FIXME
            //throw new \PHPCR\ItemNotFoundException("No item found at ".$path);
        }

        try {
            
            //TODO Soft Delete??
            
            $regex = new \MongoRegex('/^' . addcslashes($path, '/') . '/'); 
            
            $coll = $this->db->selectCollection(self::COLLNAME_NODES);
            $qb = $coll->createQueryBuilder()
                       ->remove()
                       ->field('path')->equals($regex)
                       ->field('w_id')->equals($this->workspaceId);
            $query = $qb->getQuery();
        
            return $query->execute();
        } catch(\Exception $e) {
            return false;
        }
    }

    /**
     * Deletes a property
     *
     * @param string $path Absolute path to identify a special item.
     * @return bool true on success
     *
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function deleteProperty($path)
    {
        $this->assertLoggedIn();
        
        $path = $this->validatePath($path);
        $parentPath = $this->getParentPath($path);
        
        $name = trim(str_replace($parentPath, '', $path), '/');
        
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        $qb = $coll->createQueryBuilder()
                   ->update()
                   ->field('props.' . $name)->unsetField()
                   ->field('path')->equals($parentPath)
                   ->field('w_id')->equals($this->workspaceId);
        $query = $qb->getQuery();
        
        return $query->execute();
    }

    /**
     * Moves a node from src to dst
     *
     * @param   string  $srcAbsPath     Absolute source path to the node
     * @param   string  $dstAbsPath     Absolute destination path (must NOT include the new node name)
     * @return void
     *
     * @link http://www.ietf.org/rfc/rfc2518.txt
     * @see \Jackalope\Workspace::moveNode
     */
    public function moveNode($srcAbsPath, $dstAbsPath)
    {
        $this->assertLoggedIn();
        
        $srcAbsPath = $this->validatePath($srcAbsPath);
        $dstAbsPath = $this->validatePath($dstAbsPath);

        if (!$this->pathExists($srcAbsPath)) {
            throw new \PHPCR\PathNotFoundException("Source path '".$srcAbsPath."' not found");
        }

        if ($this->pathExists($dstAbsPath)) {
            throw new \PHPCR\ItemExistsException("Cannot copy to destination path '" . $dstAbsPath . "' that already exists.");
        }

        if (!$this->pathExists($this->getParentPath($dstAbsPath))) {
            throw new \PHPCR\PathNotFoundException("Parent of the destination path '" . $this->getParentPath($dstAbsPath) . "' has to exist.");
        }
        
        try {

            $regex = new \MongoRegex('/^' . addcslashes($srcAbsPath, '/') . '/'); 
            
            $coll = $this->db->selectCollection(self::COLLNAME_NODES);
            $qb = $coll->createQueryBuilder()
                       ->field('path')->equals($regex)
                       ->field('w_id')->equals($workspaceId);
    
            $query = $qb->getQuery();
            $nodes = $query->getIterator();
            
            foreach ($nodes as $node){
                $newPath = str_replace($srcAbsPath, $dstAbsPath, $node['path']);
                
                $node['path'] = $newPath;
                $node['parent'] = $this->getParentPath($newPath);
                
                $coll->save($node);
            }
            
        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * Get parent path of a path.
     * 
     * @param  string $path
     * @return string
     */
    private function getParentPath($path)
    {
        $parentPath = implode('/', array_slice(explode('/', $path), 0, -1));
        return ($parentPath != '') ? $parentPath : '/';
    }

    private function validateNode(\PHPCR\NodeInterface $node, \PHPCR\NodeType\NodeTypeDefinitionInterface $def)
    {
        foreach ($def->getDeclaredChildNodeDefinitions() AS $childDef) {
            /* @var $childDef \PHPCR\NodeType\NodeDefinitionInterface */
            if (!$node->hasNode($childDef->getName())) {
                if ($childDef->isMandatory() && !$childDef->isAutoCreated()) {
                    throw new \PHPCR\RepositoryException(
                        "Child " . $child->getName() . " is mandatory, but is not present while ".
                        "saving " . $def->getName() . " at " . $node->getPath()
                    );
                } else if ($childDef->isAutoCreated()) {

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
                if ($propertyDef->isMandatory() && !$propertyDef->isAutoCreated()) {
                    throw new \PHPCR\RepositoryException(
                        "Property " . $propertyDef->getName() . " is mandatory, but is not present while ".
                        "saving " . $def->getName() . " at " . $node->getPath()
                    );
                } else if ($propertyDef->isAutoCreated()) {
                    $defaultValues = $propertyDef->getDefaultValues();
                    $node->setProperty(
                        $propertyDef->getName(),
                        $propertyDef->isMultiple() ? $defaultValues : (isset($defaultValues[0]) ? $defaultValues[0] : null),
                        $propertyDef->getRequiredType()
                    );
                }

                if ($node->hasNode($propertyDef->getName())) {
                    throw new \PHPCR\RepositoryException(
                        "Node " . $node->getPath() . " has child with name ".
                        $propertyDef->getName() . " but its node type '". $def->getName() . "' defines a ".
                        "property with this name."
                    );
                }
            }
        }
    }

    /**
     * Recursively store a node and its children to the given absolute path.
     *
     * Transport stores the node at its path, with all properties and all children
     *
     * @param \PHPCR\NodeInterface $node the node to store
     * @return bool true on success
     *
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function storeNode(\PHPCR\NodeInterface $node)
    {
        $this->assertLoggedIn();
        
        $path = $node->getPath();
        $path = $this->validatePath($path);
        
        // getting the property definitions is a copy of the DoctrineDBAL 
        // implementation - maybe there is a better way?
        
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

        $popertyDefs = array();
        foreach ($nodeTypes AS $nodeType) {
            /* @var $nodeType \PHPCR\NodeType\NodeTypeDefinitionInterface */
            foreach ($nodeType->getDeclaredPropertyDefinitions() AS $itemDef) {
                /* @var $itemDef \PHPCR\NodeType\ItemDefinitionInterface */
                if ($itemDef->getName() == '*') {
                    continue;
                }
                
                if (isset($popertyDefs[$itemDef->getName()])) {
                    throw new \PHPCR\RepositoryException("DoctrineTransport does not support child/property definitions for the same subpath.");
                }
                $popertyDefs[$itemDef->getName()] = $itemDef;
            }
            $this->validateNode($node, $nodeType);
        }

        $properties = $node->getProperties();

        try {
            $nodeIdentifier = (isset($properties['jcr:uuid'])) ? $properties['jcr:uuid']->getNativeValue() :  UUIDHelper::generateUUID();
            
            $props = array();
            foreach ($properties AS $property) {
                $data = $this->decodeProperty($property, $popertyDefs);
                if (!empty($data)) {
                    $props[$property->getName()] = $data;  
                }
            }
            
            $data = array(
                '_id' => new \MongoBinData($nodeIdentifier, \MongoBinData::UUID),
                'path' => $path,
                'parent' => $this->getParentPath($path),
                'w_id'  => $this->workspaceId,
                'type' => isset($properties['jcr:primaryType']) ? $properties['jcr:primaryType']->getValue() : 'nt:unstructured',
                'props' => $props
            );
            
            $coll = $this->db->selectCollection(self::COLLNAME_NODES);
            if (!$this->pathExists($path)) {
                $coll->insert($data);
            }else{
                $qb = $coll->createQueryBuilder()
                           ->update()
                           ->setNewObj($data)
                           ->field('path')->equals($path)
                           ->field('w_id')->equals($this->workspaceId);
                $query = $qb->getQuery();
                $query->execute();  //FIXME use _id for update?
            }
            
            if ($node->hasNodes()) {
                // TODO save all chiles?
            }            
            
        } catch(\Exception $e) {
            throw new \PHPCR\RepositoryException("Storing node " . $node->getPath() . " failed: " . $e->getMessage(), null, $e);
        }

        return true;
    }

    /**
     * Stores a property to the given absolute path
     *
     * @param string $path Absolute path to identify a specific property.
     * @param \PHPCR\PropertyInterface
     * @return bool true on success
     *
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function storeProperty(\PHPCR\PropertyInterface $property)
    {   
        $this->assertLoggedIn();
        
        $path = $property->getPath();
        $path = $this->validatePath($path);
        
        $parent = $property->getParent();
        $parentPath = $this->validatePath($parent->getPath());
        
        try {
        
            $data = $this->decodeProperty($property);
        
        } catch(\Exception $e) {
            //echo $path . "\n";
            //echo $e->getMessage() . "\n";
            //echo $e->getTraceAsString() . "\n";
            //exit();
        }
        
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        $qb = $coll->createQueryBuilder()
                   ->update()
                   ->upsert()
                   ->field('props.' . $property->getName())->set($data)
                   ->field('path')->equals($parentPath)
                   ->field('w_id')->equals($this->workspaceId);
        $query = $qb->getQuery();
        
        return $query->execute();
    }
    
    /**
     * "Decode" PHPCR property to MongoDB property
     * 
     * @param $property
     * @param $propDefinitions
     * @return array|null
     */
    private function decodeProperty(\PHPCR\PropertyInterface $property, $propDefinitions = array())
    {
        $path = $property->getPath();
        $path = $this->validatePath($path);
        $name = explode("/", $path);
        $name = end($name);
        
        if ($name == "jcr:uuid" || $name == "jcr:primaryType") {
            return null;
        }

        if (!$property->isModified() && !$property->isNew()) {
            return null;
        }
        
        if (($property->getType() == PropertyType::REFERENCE || $property->getType() == PropertyType::WEAKREFERENCE) &&
            !$property->getNode()->isNodeType('mix:referenceable')) {
            throw new \PHPCR\ValueFormatException('Node ' . $property->getNode()->getPath() . ' is not referencable');
        }
         
        $isMultiple = $property->isMultiple();
        if (isset($propDefinitions[$name])) {
            /* @var $propertyDef \PHPCR\NodeType\PropertyDefinitionInterface */
            $propertyDef = $propDefinitions[$name];
            if ($propertyDef->isMultiple() && !$isMultiple) {
                $isMultiple = true;
            } else if (!$propertyDef->isMultiple() && $isMultiple) {
                throw new \PHPCR\ValueFormatException(
                    'Cannot store property ' . $property->getPath() . ' as array, '.
                    'property definition of nodetype ' . $propertyDef->getDeclaringNodeType()->getName() .
                    ' requests a single value.'
                );
            }

            if ($propertyDef !== \PHPCR\PropertyType::UNDEFINED) {
                // TODO: Is this the correct way? No side effects while initializtion?
                $property->setValue($property->getValue(), $propertyDef->getRequiredType());
            }

            foreach ($propertyDef->getValueConstraints() AS $valueConstraint) {
                // TODO: Validate constraints
            }
        }
        
        $data = array(
            'multi' => $isMultiple,
        );
        
        $typeId = $property->getType();
        $type = PropertyType::nameFromValue($typeId);
        
        $data['type'] = $type;

        $binaryData = null;
        switch ($typeId) {
            case \PHPCR\PropertyType::NAME:
            case \PHPCR\PropertyType::URI:
            case \PHPCR\PropertyType::WEAKREFERENCE:
            case \PHPCR\PropertyType::REFERENCE:
            case \PHPCR\PropertyType::PATH:
                $values = $property->getString();
                break;
            case \PHPCR\PropertyType::DECIMAL:
                $values = $property->getDecimal();
                break;
            case \PHPCR\PropertyType::STRING:
                $values = $property->getString();
                break;
            case \PHPCR\PropertyType::BOOLEAN:
                $values = $property->getBoolean();
                break;
            case \PHPCR\PropertyType::LONG:
                $values = $property->getLong();
                break;
            case \PHPCR\PropertyType::BINARY:
                if ($property->isMultiple()) {
                    foreach ((array)$property->getBinary() AS $binary) {
                        $binary = stream_get_contents($binary);
                        $binaryData[] = $binary;
                        $values[] = strlen($binary);
                    }
                } else {
                    $binary = stream_get_contents($property->getBinary());
                    $binaryData[] = $binary;
                    $values = strlen($binary);
                }
                break;
            case \PHPCR\PropertyType::DATE:
                if ($property->isMultiple()) {
                    $dates = $property->getDate() ?: new \DateTime('now');
                    foreach ((array)$dates AS $date) {
                        $value = array(
                            'date' => new \MongoDate($date->getTimestamp()), 
                            'timezone' => $date->getTimezone()->getName()
                        );
                        $values[] = $value;
                    }
                } else {
                    $date = $property->getDate() ?: new \DateTime('now');
                    $values = array(
                        'date' => new \MongoDate($date->getTimestamp()), 
                        'timezone' => $date->getTimezone()->getName()
                    );
                }
                break;
            case \PHPCR\PropertyType::DOUBLE:
                $values = $property->getDouble();
                break;
        }

        if ($isMultiple) {
            $data['value'] = array();
            foreach ((array)$values AS $value) {
                $this->assertValidPropertyValue($data['type'], $value, $path);

                $data['value'][] = $value;
            }
        } else {
            $this->assertValidPropertyValue($data['type'], $values, $path);

            $data['value'] = $values;
        }
        
        
        if ($binaryData) {
            
            try {    
                foreach ($binaryData AS $idx => $binary) {
                    $grid = $this->db->getGridFS();
                    $grid->getMongoCollection()->storeBytes($binary, array(
                        'path' => $path,
                        'w_id' => $this->workspaceId,
                        'idx'  => $idx
                    ));
                    
                }
                
            } catch (\Exception $e) {
                throw $e;
            }
        }
        
        return $data;
    }

    /**
     * Validation if all the data is correct before writing it into the database.
     *
     * @param int $type
     * @param mixed $value
     * @param string $path
     * @return void
     * 
     * @throws \PHPCR\ValueFormatException
     */
    private function assertValidPropertyValue($type, $value, $path)
    {
        if ($type === \PHPCR\PropertyType::NAME) {
            if (strpos($value, ":") !== false) {
                list($prefix, $localName) = explode(":", $value);

                $this->getNamespaces();
                if (!isset($this->validNamespacePrefixes[$prefix])) {
                    throw new \PHPCR\ValueFormatException("Invalid JCR NAME at " . $path . ": The namespace prefix " . $prefix . " does not exist.");
                }
            }
        } else if ($type === \PHPCR\PropertyType::PATH) {
            if (!preg_match('((/[a-zA-Z0-9:_-]+)+)', $value)) {
                throw new \PHPCR\ValueFormatException("Invalid PATH at " . $path .": Segments are seperated by / and allowed chars are a-zA-Z0-9:_-");
            }
        } else if ($type === \PHPCR\PropertyType::URI) {
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

    /**
     * Get the node path from a JCR uuid
     *
     * @param string $uuid the id in JCR format
     * @return string Absolute path to the node
     *
     * @throws \PHPCR\ItemNotFoundException if the backend does not know the uuid
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function getNodePathForIdentifier($uuid)
    {
        $this->assertLoggedIn();
        
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        
        $qb = $coll->createQueryBuilder()
                   ->field('_id')->equals(new \MongoBinData($uuid, \MongoBinData::UUID))
                   ->field('w_id')->equals($this->workspaceId);
        
        $query = $qb->getQuery();
        $node = $query->getSingleResult();
        
        if (empty($node)) {
            throw new \PHPCR\ItemNotFoundException("no item found with uuid ".$uuid);
        }
        return $node['path'];
    }

    /**
     * Returns node types
     * @param array nodetypes to request
     * @return dom with the definitions
     * @throws \PHPCR\RepositoryException if not logged in
     */
    public function getNodeTypes($nodeTypes = array())
    {
        // TODO: Filter for the passed nodetypes
        // TODO: Check database for user node-types.

        return PHPCR2StandardNodeTypes::getNodeTypeData();
    }

    /**
     * Register namespaces and new node types or update node types based on a
     * jackrabbit cnd string
     *
     * @see \Jackalope\NodeTypeManager::registerNodeTypesCnd
     *
     * @param $cnd The cnd string
     * @param boolean $allowUpdate whether to fail if node already exists or to update it
     * @return bool true on success
     */
    public function registerNodeTypesCnd($cnd, $allowUpdate)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    /**
     * @param array $types a list of \PHPCR\NodeType\NodeTypeDefinitionInterface objects
     * @param boolean $allowUpdate whether to fail if node already exists or to update it
     * @return bool true on success
     */
    public function registerNodeTypes($types, $allowUpdate)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    public function setNodeTypeManager($nodeTypeManager)
    {
        $this->nodeTypeManager = $nodeTypeManager;
    }

    public function cloneFrom($srcWorkspace, $srcAbsPath, $destAbsPath, $removeExisting)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    public function getBinaryStream($path)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    public function getProperty($path)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    public function query(\PHPCR\Query\QueryInterface $query)
    {
        throw new \Jackalope\NotImplementedException("Not implemented yet");
    }

    /**
     * Register a new namespace.
     *
     * Validation based on what was returned from getNamespaces has already
     * happened in the NamespaceRegistry.
     *
     * The transport is however responsible of removing an existing prefix for
     * that uri, if one exists. As well as removing the current uri mapped to
     * this prefix if this prefix is already existing.
     *
     * @param string $prefix The prefix to be mapped.
     * @param string $uri The URI to be mapped.
     */
    public function registerNamespace($prefix, $uri)
    {
        $coll = $this->db->selectCollection(self::COLLNAME_NAMESPACES);
        $namespace = array(
            'prefix' => $prefix,
            'uri' => $uri,
        );
        $coll->insert($namespace);
    }

    /**
     * Unregister an existing namespace.
     *
     * Validation based on what was returned from getNamespaces has already
     * happened in the NamespaceRegistry.
     *
     * @param string $prefix The prefix to unregister.
     */
    public function unregisterNamespace($prefix)
    {
        $coll = $this->db->selectCollection(self::COLLNAME_NAMESPACES);
        $qb = $coll->createQueryBuilder()
                   ->field('prefix')->equals($prefix);
        
        $query = $qb->getQuery();
        $coll->remove($query);
    }

    /**
     * Returns the path of all accessible REFERENCE properties in the workspace that point to the node
     *
     * @param string $path
     * @param string $name name of referring REFERENCE properties to be returned; if null then all referring REFERENCEs are returned
     * @return array
     */
    public function getReferences($path, $name = null)
    {
        return $this->getNodeReferences($path, $name, false);
    }

    /**
     * Returns the path of all accessible WEAKREFERENCE properties in the workspace that point to the node
     *
     * @param string $path
     * @param string $name name of referring WEAKREFERENCE properties to be returned; if null then all referring WEAKREFERENCEs are returned
     * @return array
     */
    public function getWeakReferences($path, $name = null)
    {
        return $this->getNodeReferences($path, $name, true);
    }

    /**
     * Returns the path of all accessible reference properties in the workspace that point to the node.
     * If $weak_reference is false (default) only the REFERENCE properties are returned, if it is true, only WEAKREFERENCEs.
     * @param string $path
     * @param string $name name of referring WEAKREFERENCE properties to be returned; if null then all referring WEAKREFERENCEs are returned
     * @param boolean $weak_reference If true return only WEAKREFERENCEs, otherwise only REFERENCEs
     * @return array
     */
    protected function getNodeReferences($path, $name = null, $weak_reference = false)
    {
        $path = $this->validatePath($path);
        $type = $weak_reference ? \PHPCR\PropertyType::TYPENAME_WEAKREFERENCE : \PHPCR\PropertyType::TYPENAME_REFERENCE;

        // FIXME query is not correct!
        $coll = $this->db->selectCollection(self::COLLNAME_NODES);
        $qb = $coll->createQueryBuilder()
                   ->field('props.$.type')->equals($type)
                   ->field('path')->equals($path)
                   ->field('w_id')->equals($this->workspaceId);
        $query = $qb->getQuery();
        
        $nodes = $query->getIterator();
        
        $references = array();
        foreach ($nodes as $node) {
            
        }
        
        /*
        $sql = "SELECT p.path, p.name FROM jcrprops p " .
               "INNER JOIN jcrnodes r ON r.identifier = p.string_data AND p.w_id = ? AND r.w_id = ?" .
               "WHERE r.path = ? AND p.type = ?";
        $properties = $this->conn->fetchAll($sql, array($this->workspaceId, $this->workspaceId, $path, $type));

        $references = array();
        foreach ($properties AS $property) {
            if ($name === null || $property['name'] == $name) {
                $references[] = "/" . $property['path'];
            }
        }*/
        
        return $references;
    }


    /**
     * Return the permissions of the current session on the node given by path.
     * The result of this function is an array of zero, one or more strings from add_node, read, remove, set_property.
     *
     * @param string $path the path to the node we want to check
     * @return array of string
     */
    public function getPermissions($path)
    {
        return array(
            \PHPCR\SessionInterface::ACTION_ADD_NODE,
            \PHPCR\SessionInterface::ACTION_READ,
            \PHPCR\SessionInterface::ACTION_REMOVE,
            \PHPCR\SessionInterface::ACTION_SET_PROPERTY
        );
    }
    
    /**
     * Validate path.
     * 
     * @param $path
     * @return string
     */
    protected function validatePath($path)
    {
        $this->ensureValidPath($path);
        
        return $path; 
    }

    /**
     * Ensure path is valid.
     * 
     * @param $path
     * 
     * @throws \PHPCR\RepositoryException if path is not well-formed or contains invalid characters 
     */
    protected function ensureValidPath($path)
    {
        if (! (strpos($path, '//') === false
              && strpos($path, '/../') === false
              && preg_match('/^[\w{}\/#:^+~*\[\]\. -]*$/i', $path))
        ) {
            throw new \PHPCR\RepositoryException('Path is not well-formed or contains invalid characters: ' . $path);
        }
    }
}