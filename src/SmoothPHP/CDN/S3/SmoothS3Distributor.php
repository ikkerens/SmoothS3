<?php

namespace SmoothPHP\CDN\S3;

use Aws\S3\S3Client;
use Exception;
use SmoothPHP\Framework\Cache\Assets\Distribution\AssetDistributor;
use SmoothPHP\Framework\Core\Lock;

class SmoothS3Distributor implements AssetDistributor {
	private $config;
	private $index;

	/* @var S3Client */
	private $client; // transient

	public function __construct(array $config) {
		$this->config = array_replace_recursive($config, [
			'version' => 'latest',
			'path'    => '/'
		]);
		foreach (['bucket', 'region'] as $param)
			if (!isset($this->config[$param]))
				throw new Exception('S3CDN \'' . $param . '\' field was not defined in config.');
	}

	private function loadCacheIndex() {
		if (file_exists(__ROOT__ . 'cache/s3-repo'))
			return json_decode(file_get_contents(__ROOT__ . 'cache/s3-repo'), true, 512, JSON_THROW_ON_ERROR);
		else
			return [];
	}

	public function __sleep() {
		return ['config'];
	}

	public function getTextURL($type, $hash, callable $contentProvider) {
		return $this->getURL($type . '/compiled.' . $hash . '.' . $type, $hash, $contentProvider);
	}

	public function getImageURL($cachedFile, $virtualPath) {
		return $this->getURL($virtualPath, md5($cachedFile), function() use (&$cachedFile) {
			return file_get_contents($cachedFile);
		});
	}

	private function getURL($file, $hash, callable $contentProvider) {
		if (!isset($this->config))
			$this->client = new S3Client($this->config);
		if (!isset($this->index))
			$this->loadCacheIndex();

		if (!isset($this->index[$file]) || $this->index[$file] != $hash)
			return $this->upload($file, $hash, $contentProvider());

		return $this->buildURL($file);
	}

	private function upload($file, $hash, $content) {
		$lock = new Lock('s3-' . $file);

		if ($lock->lock()) {
			$this->client->putObject([
				'Bucket' => $this->config['bucket'],
				'Key'    => $this->config['path'] . $file,
				'Body'   => $content
			]);
			$this->index[$file] = $hash;

			$fh = fopen(__ROOT__ . 'cache/s3-repo', 'w');
			fwrite($fh, json_encode($this->index));
			fclose($fh);

			$lock->unlock();
		}

		return $this->buildURL($file);
	}

	private function buildURL($file) {
		return $this->client->getObjectUrl($this->config['bucket'], $this->config['path'] . $file);
	}
}