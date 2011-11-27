§<?php

$vendorDir = __DIR__.'/../lib/phpcr-utils/lib/vendor';
require_once $vendorDir.'/Symfony/Component/ClassLoader/UniversalClassLoader.php';

$doctrineCommonDir = __DIR__.'/../lib/vendor/mongodb/lib/vendor/doctrine-common/lib';
$doctrineMongoDBDir = __DIR__.'/../lib/vendor/mongodb/lib';
$classLoader = new \Symfony\Component\ClassLoader\UniversalClassLoader();
$classLoader->register();

$classLoader->registerNamespaces(array(
    'Jackalope' => __DIR__.'/',
    'PHPCR'   => array(__DIR__.'/../lib/phpcr-utils/src', __DIR__.'/../lib/phpcr/src'),
    'Symfony\Component\Console' => __DIR__.'/../lib/phpcr-utils/lib/vendor',
    'Symfony\Component\ClassLoader' => __DIR__.'/../lib/phpcr-utils/lib/vendor',
    'Doctrine' => array($doctrineCommonDir, $doctrineMongoDBDir),
));
