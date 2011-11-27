<?php
/** make sure we get ALL infos from php */
error_reporting(E_ALL | E_STRICT);

// PHPUnit 3.4 compat
if (method_exists('PHPUnit_Util_Filter', 'addDirectoryToFilter')) {
    PHPUnit_Util_Filter::addDirectoryToFilter(__DIR__);
    PHPUnit_Util_Filter::addFileToFilter(__DIR__.'/../src/Jackalope/Transport/curl.php');
}

/**
 * Bootstrap file for jackalope mongodb tests
 *
 * If you want to overwrite the defaults, you can to specify the autoloader and doctrine sources
 */
if (isset($GLOBALS['phpcr.doctrine.loader'])) {
    require_once $GLOBALS['phpcr.doctrine.loader'];
    // Make sure we have the necessary config
    $necessaryConfigValues = array(
        'phpcr.doctrine.loader',
        'phpcr.doctrine.commondir',
        'phpcr.doctrine.mongodbdir',
        'phpcr.doctrine.mongodb.server',
        'phpcr.doctrine.mongodb.dbname',
    );
    
    foreach ($necessaryConfigValues as $val) {
        if (empty($GLOBALS[$val])) {
            die('Please set '.$val.' in your phpunit.xml.' . "\n");
        }
    }
    
    $loader = new \Doctrine\Common\ClassLoader("Doctrine\Common", $GLOBALS['phpcr.doctrine.commondir']);
    $loader->register();

    $loader = new \Doctrine\Common\ClassLoader("Doctrine\MongoDB", $GLOBALS['phpcr.doctrine.mongodbdir']);
    $loader->register();
}

/** 
 * autoloader: jackalope-api-tests relies on an autoloader.
 */
require_once(dirname(__FILE__) . '/../src/autoload.mongodb.dist.php');

### Load classes needed for jackalope unit tests ###
require 'Jackalope/TestCase.php';
require 'Jackalope/Transport/Jackrabbit/JackrabbitTestCase.php';
require 'Jackalope/Transport/DoctrineDBAL/DoctrineDBALTestCase.php';
require 'Jackalope/Transport/MongoDB/MongoDBTestCase.php';

### Load the implementation loader class ###
require 'inc/MongoDBImplementationLoader.php';

$dbConn = new \Doctrine\MongoDB\Connection($GLOBALS['phpcr.doctrine.mongodb.server']);
$db = $dbConn->selectDatabase($GLOBALS['phpcr.doctrine.mongodb.dbname']);


/*
 * constants for the repository descriptor test for JCR 1.0/JSR-170 and JSR-283 specs
 */

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
