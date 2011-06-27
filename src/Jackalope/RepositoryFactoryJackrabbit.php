<?php

namespace Jackalope;

use PHPCR\RepositoryFactoryInterface;

/**
 * This factory creates repositories with the jackrabbit transport
 *
 * Use repository factory based on parameters (the parameters below are examples):
 *
 *    $factory = new \Jackalope\RepositoryFactoryJackrabbit;
 *
 *    $parameters = array('' => 'http://localhost:8080/server/');
 *    $repo = $factory->getRepository($parameters);
 *
 * @api
 */
class RepositoryFactoryJackrabbit implements RepositoryFactoryInterface
{
    private $required = array(
        'jackalope.jackrabbit_uri' => 'string (required): Path to the jackrabbit server',
    );
    private $optional = array(
        'jackalope.factory' => 'string or object: Use a custom factory class for Jackalope objects',
        'jackalope.default_header' => 'string: Set a default header to send on each request to the backend',
        'jackalope.jackrabbit_expect' => 'boolean: Send the "Expect: 100-continue" header on larger PUT and POST requests',
    );

    /**
     * Attempts to establish a connection to a repository using the given
     * parameters.
     *
     *
     *
     * @param array|null $parameters string key/value pairs as repository arguments or null if a client wishes
     *                               to connect to a default repository.
     * @return \PHPCR\RepositoryInterface a repository instance or null if this implementation does
     *                                    not understand the passed parameters
     * @throws \PHPCR\RepositoryException if no suitable repository is found or another error occurs.
     * @api
     */
    function getRepository(array $parameters = null) {
        if (null == $parameters) {
            return null;
        }
        // TODO: check if all required parameters specified

        if (isset($parameters['jackalope.factory'])) {
            $factory = is_object($parameters['jackalope.factory']) ?
                                 $parameters['jackalope.factory'] :
                                 new $parameters['jackalope.factory'];
        } else {
            $factory = new Factory();
        }

        $uri = $parameters['jackalope.jackrabbit_uri'];
        if ('/' !== substr($uri, -1, 1)) {
            $uri .= '/';
        }

        $transport = $factory->get('Transport\Davex\Client', array($uri));
        if (isset($parameters['jackalope.default_header'])) {
            $transport->setDefaultHeader($parameters['jackalope.default_header']);
        }
        if (isset($parameters['jackalope.jackrabbit_expect'])) {
            $transport->sendExpect($parameters['jackalope.jackrabbit_expect']);
        }

        return new Repository($factory, null, $transport);
    }

    /**
     * Get the list of configuration options that can be passed to getRepository
     *
     * The description string should include whether the key is mandatory or
     * optional.
     *
     * @return array hash map of configuration key => english description
     */
    function getConfigurationKeys() {
        return array_merge($this->required, $this->optional);
    }
}
