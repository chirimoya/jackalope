<?php

// PHPUnit 3.4 compat
if (method_exists('PHPUnit_Util_Filter', 'addDirectoryToFilter')) {
    PHPUnit_Util_Filter::addDirectoryToFilter(__DIR__);
    PHPUnit_Util_Filter::addFileToFilter(__DIR__.'/../src/Jackalope/Transport/curl.php');
}

/**
 * Bootstrap file for jackalope
 *
 * This file does some basic stuff that's project specific.
 *
 * function getRepository(config) which returns the repository
 * function getPHPCRSession(config) which returns the session
 *
 * TODO: remove the following once it has been moved to a base file
 * function getSimpleCredentials(user, password) which returns simpleCredentials
 *
 * constants necessary to the JCR 1.0/JSR-170 and JSR-283 specs
 */

// Make sure we have the necessary config
$necessaryConfigValues = array('phpcr.workspace', 'phpcr.transport', 'phpcr.doctrine.loader', 'phpcr.doctrine.commondir', 'phpcr.doctrine.mongodbdir');
foreach ($necessaryConfigValues as $val) {
    if (empty($GLOBALS[$val])) {
        die('Please set '.$val.' in your phpunit.xml.' . "\n");
    }
}

require_once($GLOBALS['phpcr.doctrine.loader']);

$loader = new \Doctrine\Common\ClassLoader("Doctrine\Common", $GLOBALS['phpcr.doctrine.commondir']);
$loader->register();

$loader = new \Doctrine\Common\ClassLoader("Doctrine\MongoDB", $GLOBALS['phpcr.doctrine.mongodbdir']);
$loader->register();

/** autoloader: jackalope-api-tests relies on an autoloader.
 */
require_once(dirname(__FILE__) . '/../src/Jackalope/autoloader.php');

$dbConn = new \Doctrine\MongoDB\Connection($GLOBALS['phpcr.doctrine.mongodb.server']);
$db = $dbConn->selectDatabase($GLOBALS['phpcr.doctrine.mongodb.dbname']);

/**
 * @return string classname of the repository factory
 */
function getRepositoryFactoryClass() {
    return 'Jackalope\RepositoryFactoryMongoDB';
}

/**
 * @return hashmap to be used with the repository factory
 */
function getRepositoryFactoryParameters($config) {
    global $db;
    
    return array(
        'jackalope.mongodb_database' => $db
    );
}

/**
 * Repository lookup is implementation specific.
 * @param config The configuration where to find the repository
 * @return the repository instance
 */
function getRepository($config) {
    global $db;
    
    if (!$db instanceof \Doctrine\MongoDB\Database || empty($config['transport'])) {
        return false;
    }
    
    if ($config['transport'] != 'mongodb') {
        throw new Exception("Don't know how to handle transport other than mongodb. (".$config['transport'].')');
    }
    
    $transport = new \Jackalope\Transport\MongoDB\Client(new \Jackalope\Factory, $db);
    return new \Jackalope\Repository(null, $transport);
}

/**
 * @param user The user name for the credentials
 * @param password The password for the credentials
 * @return the simple credentials instance for this implementation with the specified username/password
 */
function getSimpleCredentials($user, $password) {
    return new \PHPCR\SimpleCredentials($user, $password);
}

/**
 * Get a session for this implementation.
 * @param config The configuration that is passed to getRepository
 * @param credentials The credentials to log into the repository. If omitted, $config['user'] and $config['pass'] is used with getSimpleCredentials
 * @return A session resulting from logging into the repository found at the $config path
 */
function getPHPCRSession($config, $credentials = null) {
    $repository = getRepository($config);
    if (isset($config['pass']) || isset($credentials)) {
        if (empty($config['workspace'])) {
            $config['workspace'] = null;
        }
        if (empty($credentials)) {
            $credentials = getSimpleCredentials($config['user'], $config['pass']);
        }
        return $repository->login($credentials, $config['workspace']);
    } elseif (isset($config['workspace'])) {
        throw new \PHPCR\RepositoryException(phpcr_suite_baseCase::NOTSUPPORTEDLOGIN);
        //return $repository->login(null, $config['workspace']);
    } else {
        throw new \PHPCR\RepositoryException(phpcr_suite_baseCase::NOTSUPPORTEDLOGIN);
        //return $repository->login(null, null);
    }
}

function getFixtureLoader($config) {
    global $db;
    return new MongoDbFixtureLoader($config['doctrine.mongodb.dbname'], __DIR__ . "/fixtures/mongodb/");
}

function resetDb() {
    global $db;
    
    $db->drop();

    $coll = $db->selectCollection(\Jackalope\Transport\MongoDB\Client::COLLNAME_WORKSPACES);
    $workspace = array(
        'name' => 'default'
    );
    $coll->insert($workspace);
    
    $coll = $db->selectCollection(\Jackalope\Transport\MongoDB\Client::COLLNAME_WORKSPACES);
    $workspace = array(
        '_id' => new \MongoId('4e00e8fea381601b08000000'),
        'name' => $GLOBALS['phpcr.workspace']
    );
    $coll->insert($workspace);
    
    $coll = $db->selectCollection(\Jackalope\Transport\MongoDB\Client::COLLNAME_NODES);
    $node = array(
        'path' => '/',
        'parent' => '-1',
        'w_id' => new \MongoId('4e00e8fea381601b08000000'),
        'type' => 'nt:unstructured',
        'props' => array()
    );
    $coll->insert($node);  
}

require_once "suite/inc/importexport.php";
class MongoDbFixtureLoader implements phpcrApiTestSuiteImportExportFixtureInterface
{
    
    private $dbname;
    private $fixturePath;

    public function __construct($dbname, $fixturePath)
    {
        $this->dbname = $dbname;
        $this->fixturePath = $fixturePath;
    }

    public function import($file)
    {
        resetDb();
        
        $file = $this->fixturePath . $file . ".json";
        
        //FIXME
        exec('mongoimport --db ' . $this->dbname . ' --collection ' . \Jackalope\Transport\MongoDB\Client::COLLNAME_NODES . ' --type json --file ' . $file . ' --jsonArray 2>&1', $out);
        
    }
}

/** some constants */

define('SPEC_VERSION_DESC', 'jcr.specification.version');
define('SPEC_NAME_DESC', 'jcr.specification.name');
define('REP_VENDOR_DESC', 'jcr.repository.vendor');
define('REP_VENDOR_URL_DESC', 'jcr.repository.vendor.url');
define('REP_NAME_DESC', 'jcr.repository.name');
define('REP_VERSION_DESC', 'jcr.repository.version');
define('LEVEL_1_SUPPORTED', 'level.1.supported');
define('LEVEL_2_SUPPORTED', 'level.2.supported');
define('OPTION_TRANSACTIONS_SUPPORTED', 'option.transactions.supported');
define('OPTION_VERSIONING_SUPPORTED', 'option.versioning.supported');
define('OPTION_OBSERVATION_SUPPORTED', 'option.observation.supported');
define('OPTION_LOCKING_SUPPORTED', 'option.locking.supported');
define('OPTION_QUERY_SQL_SUPPORTED', 'option.query.sql.supported');
define('QUERY_XPATH_POS_INDEX', 'query.xpath.pos.index');
define('QUERY_XPATH_DOC_ORDER', 'query.xpath.doc.order');
