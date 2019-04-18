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
		$this->config = array_replace_recursive([
			'version' => 'latest',
			'path'    => '',
		], $config);
		foreach (['bucket', 'region', 'domain'] as $param)
			if (!isset($this->config[$param]))
				throw new Exception('S3CDN \'' . $param . '\' field was not defined in config.');
	}

	private function loadCacheIndex() {
		if (file_exists(__ROOT__ . 'cache/s3-repo'))
			$this->index = json_decode(file_get_contents(__ROOT__ . 'cache/s3-repo'), true);
		else
			$this->index = [];
	}

	public function __sleep() {
		return ['config'];
	}

	public function getTextURL($type, $hash, callable $contentProvider) {
		return $this->getURL($type . '/compiled.' . $hash . '.' . $type, $type == 'js' ? 'text/javascript' : 'text/css', $hash, $contentProvider);
	}

	public function getImageURL($cachedFile, $virtualPath) {
		if ($virtualPath[0] == '/')
			$virtualPath = substr($virtualPath, 1);
		return $this->getURL($virtualPath, image_type_to_mime_type(exif_imagetype($cachedFile)), md5($cachedFile), function () use (&$cachedFile) {
			return file_get_contents($cachedFile);
		});
	}

	private function getURL($file, $mime, $hash, callable $contentProvider) {
		if (!isset($this->index))
			$this->loadCacheIndex();

		if (!isset($this->index[$file]) || $this->index[$file] != $hash)
			return $this->upload($file, $mime, $hash, $contentProvider());

		return $this->buildURL($file);
	}

	private function upload($file, $mime, $hash, $content) {
		$lock = new Lock('s3-' . md5($file));

		if ($lock->lock()) {
			if (!isset($this->client))
				$this->client = new S3Client($this->config);

			$this->client->putObject([
				'Bucket'      => $this->config['bucket'],
				'Key'         => last($this->config['path']) . $file,
				'Body'        => $content,
				'ContentType' => $mime,
				'ACL'         => 'public-read'
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
		return $this->config['domain'] . $this->config['path'] . $file;
	}
}