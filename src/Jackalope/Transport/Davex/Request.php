<?php
/**
 * Request class for the Davex protocol
 *
 * @license http://www.apache.org/licenses/LICENSE-2.0  Apache License Version 2.0, January 2004
 *   Licensed under the Apache License, Version 2.0 (the "License");
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

namespace Jackalope\Transport\Davex;
use Jackalope\Transport\curl;

/**
 * Request class for the Davex protocol
 *
 * @package jackalope
 * @subpackage transport
 */
class Request
{
    /**
     * Name of the user agent to be exposed to a client.
     * @var string
     */
    const USER_AGENT = 'jackalope-php/1.0';

    /**
     * Identifier of the 'GET' http request method.
     * @var string
     */
    const GET = 'GET';

    /**
     * Identifier of the 'PUT' http request method.
     * @var string
     */
    const PUT = 'PUT';

    /**
     * Identifier of the 'MKCOL' http request method.
     * @var string
     */
    const MKCOL = 'MKCOL';

    /**
     * Identifier of the 'DELETE' http request method.
     * @var string
     */
    const DELETE = 'DELETE';

    /**
     * Identifier of the 'REPORT' http request method.
     * @var string
     */
    const REPORT = 'REPORT';

    /**
     * Identifier of the 'SEARCH' http request method.
     * @var string
     */
    const SEARCH = 'SEARCH';

    /**
     * Identifier of the 'PROPFIND' http request method.
     * @var string
     */
    const PROPFIND = 'PROPFIND';

    /**
     * Identifier of the 'PROPPATCH' http request method.
     * @var string
     */
    const PROPPATCH = 'PROPPATCH';

    /**
     * Identifier of the 'COPY' http request method.
     * @var string
     */
    const COPY = 'COPY';

    /**
     * Identifier of the 'MOVE' http request method.
     * @var string
     */
    const MOVE = 'MOVE';

    /**
     * Identifier of the 'CHECKIN' http request method.
     * @var string
     */
    const CHECKIN = 'CHECKIN';

    /**
     * Identifier of the 'CHECKOUT' http request method.
     * @var string
     */
    const CHECKOUT = 'CHECKOUT';

    /**
     * Identifier of the 'UPDATE' http request method.
     * @var string
     */
    const UPDATE = 'UPDATE';

    /** @var string     Possible argument for {@link setDepth()} */
    const INFINITY = 'infinity';

    /**
     * @var \Jackalope\Transport\curl
     */
    protected $curl;

    /**
     * Name of the request method to be used.
     * @var string
     */
    protected $method;

    /**
     * Url(s) to get/post/..
     * @var array
     */
    protected $uri;

    /**
     * Set of credentials necessary to connect to the server or else.
     * @var \PHPCR\CredentialsInterface
     */
    protected $credentials;

    /**
     * Request content-type
     * @var string
     */
    protected $contentType = 'text/xml; charset=utf-8';

    /**
     * How far the request should go, default is 0
     * @var int
     */
    protected $depth = 0;

    /**
     * Posted content for methods that require it
     * @var string
     */
    protected $body = '';

    /** @var array[]string  A list of additional HTTP headers to be sent */
    protected $additionalHeaders = array();

    /**
     * Initiaties the NodeTypes request object.
     *
     * @param object $factory Ignored for now, as this class does not create objects
     * TODO: document other parameters
     */
    public function __construct($factory, $curl, $method, $uri)
    {
        $this->curl = $curl;
        $this->method = $method;
        $this->setUri($uri);
    }

    public function setCredentials($creds)
    {
        $this->credentials = $creds;
    }

    public function setContentType($contentType)
    {
        $this->contentType = (string) $contentType;
    }

    /**
     * @param   int|string  $depth
     */
    public function setDepth($depth)
    {
        $this->depth = $depth;
    }

    public function setBody($body)
    {
        $this->body = (string) $body;
    }

    public function setMethod($method)
    {
        $this->method = $method;
    }

    public function setUri($uri)
    {
        if (!is_array($uri)) {
            $this->uri = array($uri => $uri);
        } else {
            $this->uri = $uri;
        }
    }

    public function addHeader($header)
    {
        $this->additionalHeaders[] = $header;
    }

    protected function prepareCurl($curl, $getCurlObject)
    {
        if ($this->credentials instanceof \PHPCR\SimpleCredentials) {
            $curl->setopt(CURLOPT_USERPWD, $this->credentials->getUserID().':'.$this->credentials->getPassword());
        } else {
            $curl->setopt(CURLOPT_USERPWD, null);
        }

        $headers = array(
            'Depth: ' . $this->depth,
            'Content-Type: '.$this->contentType,
            'User-Agent: '.self::USER_AGENT
        );
        $headers = array_merge($headers, $this->additionalHeaders);

        $curl->setopt(CURLOPT_RETURNTRANSFER, true);
        $curl->setopt(CURLOPT_CUSTOMREQUEST, $this->method);

        $curl->setopt(CURLOPT_HTTPHEADER, $headers);
        $curl->setopt(CURLOPT_POSTFIELDS, $this->body);
        if ($getCurlObject) {
            $curl->parseResponseHeaders();
        }
        return $curl;
    }

    /**
     * Requests the data to be identified by a formerly prepared request.
     *
     * Prepares the curl object, executes it and checks
     * for transport level errors, throwing the appropriate exceptions.
     *
     * @param bool $getCurlObject if to return the curl object instead of the response
     *
     * @return string|array of XML representation of the response.
     */
    public function execute($getCurlObject = false, $forceMultiple = false)
    {
        if (!$forceMultiple && count($this->uri) === 1) {
            return $this->singleRequest($getCurlObject);
        }
        return $this->multiRequest($getCurlObject);
    }

    /**
     * Requests the data for multiple requests
     *
     * @param bool $getCurlObject if to return the curl object instead of the response
     *
     * @return array of XML representations of responses or curl objects.
     */
    protected function multiRequest($getCurlObject = false)
    {
        $mh = curl_multi_init();

        $curls = array();
        foreach ($this->uri as $absPath => $uri) {
            $tempCurl = new curl($uri);
            $tempCurl = $this->prepareCurl($tempCurl, $getCurlObject);
            $curls[$absPath] = $tempCurl;
            curl_multi_add_handle($mh, $tempCurl->getCurl());
        }

        $active = null;

        do {
            $mrc = curl_multi_exec($mh, $active);
        } while ($active || $mrc == CURLM_CALL_MULTI_PERFORM);

        while ($active && CURLM_OK == $mrc) {
            if (-1 != curl_multi_select($mh)) {
                do {
                    $mrc = curl_multi_exec($mh, $active);
                } while (CURLM_CALL_MULTI_PERFORM == $mrc);
            }
        }

        $responses = array();
        foreach ($curls as $key => $curl) {
            if (empty($failed)) {
                $httpCode = $curl->getinfo(CURLINFO_HTTP_CODE);
                if ($httpCode >= 200 && $httpCode < 300) {
                    if ($getCurlObject) {
                        $responses[$key] = $curl;
                    } else {
                        $responses[$key] = curl_multi_getcontent($curl->getCurl());
                    }
                }
            }
            curl_multi_remove_handle($mh, $curl->getCurl());
        }
        curl_multi_close($mh);
        return $responses;
    }

    /**
     * Requests the data for a single requests
     *
     * @param bool $getCurlObject if to return the curl object instead of the response
     *
     * @return string XML representation of a response or curl object.
     */
    protected function singleRequest($getCurlObject)
    {
        if ($this->credentials instanceof \PHPCR\SimpleCredentials) {
            $this->curl->setopt(CURLOPT_USERPWD, $this->credentials->getUserID().':'.$this->credentials->getPassword());
        } else {
            $this->curl->setopt(CURLOPT_USERPWD, null);
        }

        $headers = array(
            'Depth: ' . $this->depth,
            'Content-Type: '.$this->contentType,
            'User-Agent: '.self::USER_AGENT
        );
        $headers = array_merge($headers, $this->additionalHeaders);

        $this->curl->setopt(CURLOPT_RETURNTRANSFER, true);
        $this->curl->setopt(CURLOPT_CUSTOMREQUEST, $this->method);
        $this->curl->setopt(CURLOPT_URL, reset($this->uri));
        $this->curl->setopt(CURLOPT_HTTPHEADER, $headers);
        $this->curl->setopt(CURLOPT_POSTFIELDS, $this->body);
        if ($getCurlObject) {
            $this->curl->parseResponseHeaders();
        }

        $response = $this->curl->exec();
        $this->curl->setResponse($response);

        $httpCode = $this->curl->getinfo(CURLINFO_HTTP_CODE);
        if ($httpCode >= 200 && $httpCode < 300) {
            if ($getCurlObject) {
                return $this->curl;
            }
            return $response;
        }
        $this->handleError($this->curl, $response, $httpCode);
    }

    /**
     * Handles errors caused by singleRequest and multiRequest
     *
     * for transport level errors, throwing the appropriate exceptions.
     * @throws \PHPCR\NoSuchWorkspaceException if it was not possible to reach the server (resolve host or connect)
     * @throws \PHPCR\ItemNotFoundException if the object was not found
     * @throws \PHPCR\RepositoryExceptions if on any other error.
     * @throws \PHPCR\PathNotFoundException if the path was not found (server returned 404 without xml response)
     *
     */
    protected function handleError($curl, $response, $httpCode)
    {
        switch ($curl->errno()) {
            case CURLE_COULDNT_RESOLVE_HOST:
            case CURLE_COULDNT_CONNECT:
                throw new \PHPCR\NoSuchWorkspaceException($curl->error());
        }

        // TODO extract HTTP status string from response, more descriptive about error

        // use XML error response if it's there
        if (substr($response, 0, 1) === '<') {
            $dom = new \DOMDocument();
            $dom->loadXML($response);
            $err = $dom->getElementsByTagNameNS(Client::NS_DCR, 'exception');
            if ($err->length > 0) {
                $err = $err->item(0);
                $errClass = $err->getElementsByTagNameNS(Client::NS_DCR, 'class')->item(0)->textContent;
                $errMsg = $err->getElementsByTagNameNS(Client::NS_DCR, 'message')->item(0)->textContent;

                $exceptionMsg = 'HTTP ' . $httpCode . ': ' . $errMsg;
                switch($errClass) {
                    case 'javax.jcr.NoSuchWorkspaceException':
                        throw new \PHPCR\NoSuchWorkspaceException($exceptionMsg);
                    case 'javax.jcr.nodetype.NoSuchNodeTypeException':
                        throw new \PHPCR\NodeType\NoSuchNodeTypeException($exceptionMsg);
                    case 'javax.jcr.ItemNotFoundException':
                        throw new \PHPCR\ItemNotFoundException($exceptionMsg);
                    case 'javax.jcr.nodetype.ConstraintViolationException':
                        throw new \PHPCR\NodeType\ConstraintViolationException($exceptionMsg);

                    //TODO: map more errors here?
                    default:

                        // try to generically "guess" the right exception class name
                        $class = substr($errClass, strlen('javax.jcr.'));
                        $class = explode('.', $class);
                        array_walk($class, function(&$ns) { $ns = ucfirst(str_replace('nodetype', 'NodeType', $ns)); });
                        $class = '\\PHPCR\\'.implode('\\', $class);

                        if (class_exists($class)) {
                            throw new $class($exceptionMsg);
                        }
                        throw new \PHPCR\RepositoryException($exceptionMsg . " ($errClass)");
                }
            }
        }

        if (404 === $httpCode) {
            throw new \PHPCR\PathNotFoundException("HTTP 404 Path Not Found: {$this->method} ".var_export($this->uri, true));
        } elseif (405 == $httpCode) {
            throw new \Jackalope\Transport\Davex\HTTPErrorException("HTTP 405 Method Not Allowed: {$this->method} ".var_export($this->uri, true), 405);
        } elseif ($httpCode >= 500) {
            throw new \PHPCR\RepositoryException("HTTP $httpCode Error from backend on: {$this->method} ".var_export($this->uri, true)."\n\n$response");
        }

        $curlError = $curl->error();

        $msg = "Unexpected error: \nCURL Error: $curlError \nResponse (HTTP $httpCode): {$this->method} ".var_export($this->uri, true)."\n\n$response";
        throw new \PHPCR\RepositoryException($msg);
    }

    /**
     * Loads the response into an DOMDocument.
     *
     * Returns a DOMDocument from the backend or throws exception.
     * Does error handling for both connection errors and dcr:exception response
     *
     * @return DOMDocument The loaded XML response text.
     */
    public function executeDom($forceMultiple = false)
    {
        $xml = $this->execute(null, $forceMultiple);

        // create new DOMDocument and load the response text.
        $dom = new \DOMDocument();
        $dom->loadXML($xml);

        return $dom;
    }

    /**
     * Loads the server response as a json string.
     *
     * Returns a decoded json string from the backend or throws exception
     *
     * @return mixed
     *
     * @throws \PHPCR\RepositoryException if the json response is not valid
     */
    public function executeJson($forceMultiple = false)
    {
        $responses = $this->execute(null, $forceMultiple);
        if (!is_array($responses)) {
            $responses = array($responses);
            $reset = true;
        }

        $json = array();
        foreach ($responses as $key => $response) {
            $json[$key] = json_decode($response);
            if (null === $json[$key] && 'null' !== strtolower($response)) {
                throw new \PHPCR\RepositoryException("Not a valid json object: \nRequest: {$this->method} {$this->uri[$key]} \nResponse: \n$response");
            }
        }
        //TODO: are there error responses in json format? if so, handle them
        if (isset($reset)) {
            return reset($json);
        }
        return $json;
    }
}
