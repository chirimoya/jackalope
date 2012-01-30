<?php

namespace Jackalope;

use ArrayIterator;
use Exception;

use PHPCR\PropertyType;
use PHPCR\RepositoryInterface;
use PHPCR\SessionInterface;
use PHPCR\NodeInterface;
use PHPCR\SimpleCredentials;
use PHPCR\CredentialsInterface;
use PHPCR\PathNotFoundException;
use PHPCR\ItemNotFoundException;
use PHPCR\ItemExistsException;
use PHPCR\UnsupportedRepositoryOperationException;

use PHPCR\Security\AccessControlException;

use Jackalope\Transport\TransportInterface;
use Jackalope\Transport\TransactionInterface;

/**
 * {@inheritDoc}
 *
 * @api
 */
class Session implements SessionInterface
{
    /**
     * A registry for all created sessions to be able to reference them by id in
     * the stream wrapper for lazy loading binary properties.
     *
     * Keys are spl_object_hash'es for the sessions which are the values
     * @var array
     */
    protected static $sessionRegistry = array();

    /**
     * The factory to instantiate objects
     * @var FactoryInterface
     */
    protected $factory;

    /**
     * @var Repository
     */
    protected $repository;
    /**
     * @var Workspace
     */
    protected $workspace;
    /**
     * @var ObjectManager
     */
    protected $objectManager;
    /**
     * @var SimpleCredentials
     */
    protected $credentials;
    /**
     * Whether this session is in logged out state and can not be used anymore
     * @var bool
     */
    protected $logout = false;
    /**
     * The namespace registry.
     *
     * It is only used to check prefixes and at setup. Session namespace
     * remapping must be handled locally.
     *
     * @var NamespaceRegistry
     */
    protected $namespaceRegistry;

    /**
     * List of local namespaces
     *
     * TODO: implement local namespace rewriting
     * see jackrabbit-spi-commons/src/main/java/org/apache/jackrabbit/spi/commons/conversion/PathParser.java and friends
     * for how this is done in jackrabbit
     */
    //protected $localNamespaces;

    /** Creates a session
     *
     * Builds the corresponding workspace instance
     *
     * @param FactoryInterface $factory the object factory
     * @param Repository $repository
     * @param string $workspaceName the workspace name that is used
     * @param SimpleCredentials $credentials the credentials that where
     *      used to log in, in order to implement Session::getUserID()
     * @param TransportInterface $transport the transport implementation
     */
    public function __construct(FactoryInterface $factory, Repository $repository, $workspaceName, SimpleCredentials $credentials, TransportInterface $transport)
    {
        $this->factory = $factory;
        $this->repository = $repository;
        $this->objectManager = $this->factory->get('ObjectManager', array($transport, $this));
        $this->workspace = $this->factory->get('Workspace', array($this, $this->objectManager, $workspaceName));
        $this->credentials = $credentials;
        $this->namespaceRegistry = $this->workspace->getNamespaceRegistry();
        self::registerSession($this);

        $transport->setNodeTypeManager($this->workspace->getNodeTypeManager());
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getRepository()
    {
        return $this->repository;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getUserID()
    {
        return $this->credentials->getUserID(); //TODO: what if its not simple credentials? what about anonymous login?
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getAttributeNames()
    {
        return $this->credentials->getAttributeNames();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getAttribute($name)
    {
        return $this->credentials->getAttribute($name);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getWorkspace()
    {
        return $this->workspace;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getRootNode()
    {
        return $this->getNode('/');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function impersonate(CredentialsInterface $credentials)
    {
        throw new UnsupportedRepositoryOperationException('Not supported');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNodeByIdentifier($id)
    {
        return $this->objectManager->getNode($id);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNodesByIdentifier($ids)
    {
        $nodesByPath = $this->objectManager->getNodes($ids);
        $nodesByUUID = array();
        foreach ($nodesByPath as $node) {
            $nodesByUUID[$node->getIdentifier()] = $node;
        }
        return new ArrayIterator($nodesByUUID);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getItem($absPath)
    {
        if (strpos($absPath,'/') !== 0) {
            throw new PathNotFoundException('It is forbidden to call getItem on session with a relative path');
        }

        if ($this->nodeExists($absPath)) {
            return $this->getNode($absPath);
        }
        return $this->getProperty($absPath);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNode($absPath)
    {
        try {
            return $this->objectManager->getNodeByPath($absPath);
        } catch (ItemNotFoundException $e) {
            throw new PathNotFoundException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNodes($absPaths)
    {
        return $this->objectManager->getNodesByPath($absPaths);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getProperty($absPath)
    {
        try {
            return $this->objectManager->getPropertyByPath($absPath);
        } catch (ItemNotFoundException $e) {
            throw new PathNotFoundException($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function itemExists($absPath)
    {
        if ($absPath == '/') {
            return true;
        }
        return $this->nodeExists($absPath) || $this->propertyExists($absPath);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function nodeExists($absPath)
    {
        if ($absPath == '/') {
            return true;
        }

        try {
            //OPTIMIZE: avoid throwing and catching errors would improve performance if many node exists calls are made
            //would need to communicate to the lower layer that we do not want exceptions
            $this->objectManager->getNodeByPath($absPath);
        } catch(ItemNotFoundException $e) {
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function propertyExists($absPath)
    {
        try {
            //OPTIMIZE: avoid throwing and catching errors would improve performance if many node exists calls are made
            //would need to communicate to the lower layer that we do not want exceptions
            $this->getProperty($absPath);
        } catch(PathNotFoundException $e) {
            return false;
        }
        return true;

    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function move($srcAbsPath, $destAbsPath)
    {
        try {
            $parent = $this->objectManager->getNodeByPath(dirname($destAbsPath));
        } catch(ItemNotFoundException $e) {
            throw new PathNotFoundException("Target path can not be found: $destAbsPath", $e->getCode(), $e);
        }

        if ($parent->hasNode(basename($destAbsPath))) {
            // TODO same-name siblings
            throw new ItemExistsException('Target node already exists at '.$destAbsPath);
        }
        if ($parent->hasProperty(basename($destAbsPath))) {
            throw new ItemExistsException('Target property already exists at '.$destAbsPath);
        }
        $this->objectManager->moveNode($srcAbsPath, $destAbsPath);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function removeItem($absPath)
    {
        $item = $this->getItem($absPath);
        $item->remove();
    }

    /**
     * {@inheritDoc}
     *
     * Wraps the save operation into a transaction if transactions are enabled
     * but we are not currently inside a transaction and rolls back on error.
     *
     * If transactions are disabled, errors on save can lead to partial saves
     * and inconsistent data.
     *
     * @api
     */
    public function save()
    {
        if ($this->getTransport() instanceof TransactionInterface) {
            $utx = $this->workspace->getTransactionManager();
        }

        if (isset($utx) && !$utx->inTransaction()) {
            // do the operation in a short transaction
            $utx->begin();
            try {
                $this->objectManager->save();
                $utx->commit();
            } catch(Exception $e) {
                // if anything goes wrong, rollback this mess
                $utx->rollback();
                // but do not eat the exception
                throw $e;
            }
        } else {
            $this->objectManager->save();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function refresh($keepChanges)
    {
        $this->objectManager->refresh($keepChanges);
    }

    /**
     * Jackalope specific hack to drop the state of the current session
     *
     * Removes all cached objects, planned changes etc without making the
     * objects aware of it. Was done as a cheap replacement for refresh
     * in testing.
     *
     * @deprecated: this will screw up major, as the user of the api can still have references to nodes. USE refresh instead!
     */
    public function clear()
    {
        trigger_error('Use Session::refresh instead, this method is extremely unsafe', E_USER_DEPRECATED);
        $this->objectManager->clear();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasPendingChanges()
    {
        return $this->objectManager->hasPendingChanges();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function hasPermission($absPath, $actions)
    {
        $actualPermissions = $this->objectManager->getPermissions($absPath);
        $requestedPermissions = explode(',', $actions);

        foreach ($requestedPermissions as $perm) {
            if (! in_array(strtolower(trim($perm)), $actualPermissions)) {
                return false;
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function checkPermission($absPath, $actions)
    {
        if (! $this->hasPermission($absPath, $actions)) {
            throw new AccessControlException($absPath);
        }
    }

    /**
     * {@inheritDoc}
     *
     * Jackalope does currently not check anything and always return true.
     *
     * @api
     */
    public function hasCapability($methodName, $target, array $arguments)
    {
        //we never determine whether operation can be performed as it is optional ;-)
        //TODO: could implement some
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function importXML($parentAbsPath, $in, $uuidBehavior)
    {
        throw new NotImplementedException('Write');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function exportSystemView($absPath, $stream, $skipBinary, $noRecurse)
    {
        $node = $this->getNode($absPath);

        fwrite($stream, '<?xml version="1.0" encoding="UTF-8"?>'."\n");
        $this->exportSystemViewRecursive($node, $stream, $skipBinary, $noRecurse, true);
    }

    /**
     * Recursively output node and all its children into the file in the system
     * view format
     *
     * @param NodeInterface $node the node to output
     * @param resource $stream The stream resource (i.e. aquired with fopen) to
     *      which the XML serialization of the subgraph will be output. Must
     *      support the fwrite method.
     * @param boolean $skipBinary A boolean governing whether binary properties
     *      are to be serialized.
     * @param boolean $noRecurse A boolean governing whether the subgraph at
     *      absPath is to be recursed.
     * @param boolean $root Whether this is the root node of the resulting
     *      document, meaning the namespace declarations have to be included in
     *      it.
     *
     * @return void
     */
    private function exportSystemViewRecursive(NodeInterface $node, $stream, $skipBinary, $noRecurse, $root=false)
    {
        fwrite($stream, '<sv:node');
        if ($root) {
            $this->exportNamespaceDeclarations($stream);
        }
        fwrite($stream, ' sv:name="'.($node->getPath() == '/' ? 'jcr:root' : htmlspecialchars($node->getName())).'">');

        // the order MUST be primary type, then mixins, if any, then jcr:uuid if its a referenceable node
        fwrite($stream, '<sv:property sv:name="jcr:primaryType" sv:type="Name"><sv:value>'.htmlspecialchars($node->getPropertyValue('jcr:primaryType')).'</sv:value></sv:property>');
        if ($node->hasProperty('jcr:mixinTypes')) {
            fwrite($stream, '<sv:property sv:name="jcr:mixinTypes" sv:type="Name">');
            foreach ($node->getPropertyValue('jcr:mixinTypes') as $type) {
                fwrite($stream, '<sv:value>'.htmlspecialchars($type).'</sv:value>');
            }
            fwrite($stream, '</sv:property>');
        }
        if ($node->isNodeType('mix:referenceable')) {
            fwrite($stream, '<sv:property sv:name="jcr:uuid" sv:type="String"><sv:value>'.$node->getIdentifier().'</sv:value></sv:property>');
        }

        foreach ($node->getProperties() as $name => $property) {
            if ($name == 'jcr:primaryType' || $name == 'jcr:mixinTypes' || $name == 'jcr:uuid') {
                // explicitly handled before
                continue;
            }
            if (PropertyType::BINARY == $property->getType() && $skipBinary) {
                // do not output binary data in the xml
                continue;
            }
            fwrite($stream, '<sv:property sv:name="'.htmlentities($name).'" sv:type="'
                                . PropertyType::nameFromValue($property->getType()).'"'
                                . ($property->isMultiple() ? ' sv:multiple="true"' : '')
                                . '>');
            $values = $property->isMultiple() ? $property->getString() : array($property->getString());

            foreach ($values as $value) {
                if (PropertyType::BINARY == $property->getType()) {
                    $val = base64_encode($value);
                } else {
                    $val = htmlspecialchars($value);
                    //TODO: can we still have invalid characters after this? if so base64 and property, xsi:type="xsd:base64Binary"
                }
                fwrite($stream, "<sv:value>$val</sv:value>");
            }
            fwrite($stream, "</sv:property>");
        }
        if (! $noRecurse) {
            foreach ($node as $child) {
                $this->exportSystemViewRecursive($child, $stream, $skipBinary, $noRecurse);
            }
        }
        fwrite($stream, '</sv:node>');
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function exportDocumentView($absPath, $stream, $skipBinary, $noRecurse)
    {
        $node = $this->getNode($absPath);

        fwrite($stream, '<?xml version="1.0" encoding="UTF-8"?>'."\n");
        $this->exportDocumentViewRecursive($node, $stream, $skipBinary, $noRecurse, true);
    }

    /**
     * Recursively output node and all its children into the file in the
     * document view format
     *
     * @param NodeInterface $node the node to output
     * @param resource $stream the resource to write data out to
     * @param boolean $skipBinary A boolean governing whether binary properties
     *      are to be serialized.
     * @param boolean $noRecurse A boolean governing whether the subgraph at
     *      absPath is to be recursed.
     * @param boolean $root Whether this is the root node of the resulting
     *      document, meaning the namespace declarations have to be included in
     *      it.
     *
     * @return void
     */
    private function exportDocumentViewRecursive(NodeInterface $node, $stream, $skipBinary, $noRecurse, $root=false)
    {
        //TODO: encode name according to spec
        $nodename = $this->escapeXmlName($node->getName());
        fwrite($stream, "<$nodename");
        if ($root) {
            $this->exportNamespaceDeclarations($stream);
        }
        foreach ($node->getProperties() as $name => $property) {
            if ($property->isMultiple()) {
                // skip multiple properties. jackrabbit does this too. cheap but whatever. use system view for a complete export
                continue;
            }
            if (PropertyType::BINARY == $property->getType()) {
                if ($skipBinary) {
                    continue;
                }
                $value = base64_encode($property->getString());
            } else {
                $value = htmlspecialchars($property->getString());
            }
            fwrite($stream, ' '.$this->escapeXmlName($name).'="'.$value.'"');
        }
        if ($noRecurse || ! $node->hasNodes()) {
            fwrite($stream, '/>');
        } else {
            fwrite($stream, '>');
            foreach ($node as $child) {
                $this->exportDocumentViewRecursive($child, $stream, $skipBinary, $noRecurse);
            }
            fwrite($stream, "</$nodename>");
        }
    }
    /**
     * Helper method for escaping node names into valid xml according to
     * the specification.
     *
     * @param string $name A node name possibly containing characters illegal
     *      in an XML document.
     *
     * @return string The name encoded to be valid xml
     */
    private function escapeXmlName($name)
    {
        $name = preg_replace('/_(x[0-9a-fA-F]{4})/', '_x005f_\\1', $name);
        return str_replace(array(' ',       '<',       '>',       '"',       "'"),
                           array('_x0020_', '_x003c_', '_x003e_', '_x0022_', '_x0027_'),
                           $name); // TODO: more invalid characters?
    }
    /**
     * Helper method to produce the xmlns:... attributes of the root node from
     * the built-in namespace registry.
     *
     * @param stream $stream the ouptut stream to write the namespaces to
     *
     * @return void
     */
    private function exportNamespaceDeclarations($stream)
    {
        foreach ($this->workspace->getNamespaceRegistry() as $key => $uri) {
            if (! empty($key)) { // no ns declaration for empty namespace
                fwrite($stream, " xmlns:$key=\"$uri\"");
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function setNamespacePrefix($prefix, $uri)
    {
        $this->namespaceRegistry->checkPrefix($prefix);
        throw new NotImplementedException('TODO: implement session scope remapping of namespaces');
        //this will lead to rewrite all names and paths in requests and replies. part of this can be done in ObjectManager::normalizePath
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNamespacePrefixes()
    {
        //TODO: once setNamespacePrefix is implemented, must take session remaps into account
        return $this->namespaceRegistry->getPrefixes();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNamespaceURI($prefix)
    {
        //TODO: once setNamespacePrefix is implemented, must take session remaps into account
        return $this->namespaceRegistry->getURI($prefix);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getNamespacePrefix($uri)
    {
        //TODO: once setNamespacePrefix is implemented, must take session remaps into account
        return $this->namespaceRegistry->getPrefix($uri);
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function logout()
    {
        //OPTIMIZATION: flush object manager to help garbage collector
        $this->logout = true;
        if ($this->getRepository()->getDescriptor(RepositoryInterface::OPTION_LOCKING_SUPPORTED)) {
            $this->getWorkspace()->getLockManager()->logout();
        }
        self::unregisterSession($this);
        $this->getTransport()->logout();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function isLive()
    {
        return ! $this->logout;
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getAccessControlManager()
    {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * {@inheritDoc}
     *
     * @api
     */
    public function getRetentionManager()
    {
        throw new UnsupportedRepositoryOperationException();
    }

    /**
     * Implementation specific: The object manager is also used by other
     * components, i.e. the QueryManager.
     *
     * @return ObjectManager the object manager associated with this session
     *
     * @private
     */
    public function getObjectManager()
    {
        return $this->objectManager;
    }

    /**
     * Implementation specific: The transport implementation is also used by
     * other components, i.e. the NamespaceRegistry
     *
     * @return TransportInterface the transport implementation associated with
     *      this session.
     *
     * @private
     */
    public function getTransport()
    {
        return $this->objectManager->getTransport();
    }

    /**
     * Implementation specific: register session in session registry for the
     * stream wrapper.
     *
     * @param Session $session the session to register
     *
     * @private
     */
    protected static function registerSession(Session $session)
    {
        $key = $session->getRegistryKey();
        self::$sessionRegistry[$key] = $session;
    }

    /**
     * Implementation specific: unregister session in session registry on
     * logout.
     *
     * @param Session $session the session to unregister
     *
     * @private
     */
    protected static function unregisterSession(Session $session)
    {
        $key = $session->getRegistryKey();
        unset(self::$sessionRegistry[$key]);
    }

    /**
     * Implementation specific: create an id for the session registry so that
     * the stream wrapper can identify it.
     *
     * @private
     *
     * @return string an id for this session
     */
    public function getRegistryKey()
    {
        return spl_object_hash($this);
    }

    /**
     * Implementation specific: get a session from the session registry for the
     * stream wrapper.
     *
     * @param string $key key for the session
     *
     * @return the session or null if none is registered with the given key
     *
     * @private
     */
    public static function getSessionFromRegistry($key)
    {
        if (isset(self::$sessionRegistry[$key])) {
            return self::$sessionRegistry[$key];
        }
    }
}
