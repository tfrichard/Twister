/*
 * Software License, Version 1.0
 *
 *  Copyright 2003 The Trustees of Indiana University.  All rights reserved.
 *
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1) All redistributions of source code must retain the above copyright notice,
 *  the list of authors in the original source code, this list of conditions and
 *  the disclaimer listed in this license;
 * 2) All redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the disclaimer listed in this license in
 *  the documentation and/or other materials provided with the distribution;
 * 3) Any documentation included with all redistributions must include the
 *  following acknowledgement:
 *
 * "This product includes software developed by the Community Grids Lab. For
 *  further information contact the Community Grids Lab at
 *  http://communitygrids.iu.edu/."
 *
 *  Alternatively, this acknowledgement may appear in the software itself, and
 *  wherever such third-party acknowledgments normally appear.
 *
 * 4) The name Indiana University or Community Grids Lab or Twister,
 *  shall not be used to endorse or promote products derived from this software
 *  without prior written permission from Indiana University.  For written
 *  permission, please contact the Advanced Research and Technology Institute
 *  ("ARTI") at 351 West 10th Street, Indianapolis, Indiana 46202.
 * 5) Products derived from this software may not be called Twister,
 *  nor may Indiana University or Community Grids Lab or Twister appear
 *  in their name, without prior written permission of ARTI.
 *
 *
 *  Indiana University provides no reassurances that the source code provided
 *  does not infringe the patent or any other intellectual property rights of
 *  any other entity.  Indiana University disclaims any liability to any
 *  recipient for claims brought by any other entity based on infringement of
 *  intellectual property rights or otherwise.
 *
 * LICENSEE UNDERSTANDS THAT SOFTWARE IS PROVIDED "AS IS" FOR WHICH NO
 * WARRANTIES AS TO CAPABILITIES OR ACCURACY ARE MADE. INDIANA UNIVERSITY GIVES
 * NO WARRANTIES AND MAKES NO REPRESENTATION THAT SOFTWARE IS FREE OF
 * INFRINGEMENT OF THIRD PARTY PATENT, COPYRIGHT, OR OTHER PROPRIETARY RIGHTS.
 * INDIANA UNIVERSITY MAKES NO WARRANTIES THAT SOFTWARE IS FREE FROM "BUGS",
 * "VIRUSES", "TROJAN HORSES", "TRAP DOORS", "WORMS", OR OTHER HARMFUL CODE.
 * LICENSEE ASSUMES THE ENTIRE RISK AS TO THE PERFORMANCE OF SOFTWARE AND/OR
 * ASSOCIATED MATERIALS, AND TO THE PERFORMANCE AND VALIDITY OF INFORMATION
 * GENERATED USING SOFTWARE.
 */

package cgl.imr.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;

import cgl.imr.config.ConfigurationException;
import cgl.imr.config.TwisterConfigurations;

/**
 * Custom class loader for MapReduce computations. A separate class loader is
 * created for every MapReduce computation and is initialized at the request of
 * the new MapReduce computation. Then the class loader remains in memory during
 * the iterative execution of the maps and reduce tasks and finally gets
 * discarded at the end of the computation. This enables the application jar
 * files to be updated without restarting the Twister runtime. The custom class
 * loader loads only the jars available in the "apps" directory and the system
 * classes.
 * 
 * @author Jaliya Ekanayake (jaliyae@gmail.com, jekanaya@cs.indiana.edu) based
 *         on a sample provided by Kalani Ruwanpathirana (kalanir@gmail.com)
 * 
 *         Now one classloader can be shared by several friend jobs, add ref
 *         count to count how many jobs are using it.
 * 
 * @author zhangbj
 * 
 * 
 */
public class CustomClassLoader extends ClassLoader {
	// classloader could be shared now, use concurrent map
	private ConcurrentMap<String, Class<?>> classes = new ConcurrentHashMap<String, Class<?>>();
	private String directory;
	// refcount
	private AtomicInteger refCount = new AtomicInteger(0);

	public CustomClassLoader() throws JarClassLoaderException {
		super(CustomClassLoader.class.getClassLoader());
		this.directory = getJarDir();
		populateClasses();
	}

	public int decreaseRef() {
		return this.refCount.decrementAndGet();
	}

	public int increaseRef() {
		return this.refCount.incrementAndGet();
	}

	public void close() {
		classes.clear();
	}

	@Override
	public Class<?> findClass(String className) throws ClassNotFoundException {
		Class<?> clazz = classes.get(className);
		if (clazz != null) {
			return clazz;
		} else if ((clazz = findSystemClass(className)) != null) {
			return clazz;
		} else {
			throw new ClassNotFoundException("Could not find class:"
					+ className);
		}
	}

	private String getJarDir() throws JarClassLoaderException {
		TwisterConfigurations configs;
		try {
			configs = TwisterConfigurations.getInstance();
		} catch (ConfigurationException e) {
			throw new JarClassLoaderException(
					"Error in loading configurations. Could not load jars.", e);
		}

		String jarDirectory = configs.getLocalAppJarDir();
		if (jarDirectory == null) {
			throw new JarClassLoaderException(
					"Application directory is empty. Could not load jars.");
		}
		return jarDirectory;
	}

	@Override
	public Class<?> loadClass(String className) throws ClassNotFoundException {
		return findClass(className);
	}

	/**
	 * Go through the classes in the application directory and add all the
	 * classes to a hash table.
	 * 
	 * @throws JarClassLoaderException
	 */
	private void populateClasses() throws JarClassLoaderException {
		File dir = new File(directory);
		File[] jars = dir.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isFile() && file.getName().endsWith(".jar");
			}
		});
		if (jars == null) {
			return;
		}
		byte classBytes[];
		Class<?> result = null;
		String className = null;
		JarInputStream jarFile;
		InputStream classInputStream;
		try {
			JarFile jar = null;
			for (File f : jars) {
				jar = new JarFile(directory + "/" + f.getName());
				jarFile = new JarInputStream(new FileInputStream(jar.getName()));
				JarEntry jarEntry;
				while (true) {
					jarEntry = jarFile.getNextJarEntry();
					if (jarEntry == null) {
						break;
					}
					if ((jarEntry.getName().endsWith(".class"))) {
						className = jarEntry.getName().replaceAll("/", "\\.")
								.replace(".class", "");
						classInputStream = jar.getInputStream(jarEntry);
						ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
						int nextValue = classInputStream.read();
						while (-1 != nextValue) {
							byteStream.write(nextValue);
							nextValue = classInputStream.read();
						}

						classBytes = byteStream.toByteArray();
						byteStream.close();
						result = defineClass(className, classBytes, 0,
								classBytes.length, null);
						classes.put(className, result);
						classInputStream.close();
					}
				}
				jarFile.close();
				jar.close();
			}
		} catch (Exception e) {
			throw new JarClassLoaderException(e);
		}
	}
}
